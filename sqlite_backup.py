#!/usr/bin/env python3
"""SQLite 数据库备份、恢复与增量快照 CLI 工具。

支持:
  - backup:   全量备份（可 gzip 压缩）+ SHA256 + 元数据记录
  - snapshot: 增量快照（按主键/rowid 记录新增/删除/修改，JSON patch）
  - restore:  从全量备份恢复（自动安全副本、dry-run）
  - apply:    将增量快照应用到目标数据库
  - verify:   校验哈希、SQLite 完整性、表结构一致性
  - history:  列出备份链路和快照依赖关系
  - diff:     两个数据库的结构/数据差异报告（可导出 Markdown）
  - prune:    按保留规则清理旧备份（保留最近N个/每天一个/每周一个）
  - init-demo:生成示例数据库 (users/orders/logs + 演示数据)
"""

from __future__ import annotations

import argparse
import datetime as dt
import gzip
import hashlib
import json
import os
import re
import shutil
import sqlite3
import sys
import tarfile
import tempfile
import uuid
from collections import OrderedDict
from contextlib import closing
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

# ----------------------------- 常量与默认配置 ----------------------------- #

BACKUP_DIR = Path("./.sqlite_backups")
META_FILE = "manifest.json"
SNAPSHOT_DIRNAME = "snapshots"
BACKUP_DIRNAME = "backups"
INDEX_NAME = "index.json"

DT_FMT = "%Y%m%d_%H%M%S"
SAFE_COPY_SUFFIX = ".pre_restore_"


# ------------------------------ 通用辅助函数 ------------------------------ #

def now_str() -> str:
    return dt.datetime.now().strftime(DT_FMT)


def now_iso() -> str:
    return dt.datetime.now().isoformat(timespec="seconds")


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def sha256_file(path: Path, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            buf = f.read(chunk)
            if not buf:
                break
            h.update(buf)
    return h.hexdigest()


def safe_exists_db(path: Path) -> bool:
    """检查是否是有效的 sqlite3 文件（不触发 WAL 等副作用）。"""
    if not path.exists() or not path.is_file():
        return False
    try:
        with open(path, "rb") as f:
            header = f.read(16)
        return header[:16] == b"SQLite format 3\x00"
    except OSError:
        return False


def connect_readonly(path: Path) -> sqlite3.Connection:
    """以只读方式打开 sqlite，避免修改源文件。"""
    uri = f"file:{path}?mode=ro"
    return sqlite3.connect(uri, uri=True)


def gzip_compress(src: Path, dst: Path) -> None:
    with open(src, "rb") as f_in, gzip.open(dst, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)


def gzip_decompress(src: Path, dst: Path) -> None:
    with gzip.open(src, "rb") as f_in, open(dst, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)


def is_gzip(path: Path) -> bool:
    try:
        with open(path, "rb") as f:
            return f.read(2) == b"\x1f\x8b"
    except OSError:
        return False


# ----------------------------- 备份索引 / 元数据 ----------------------------- #

class Store:
    """负责备份目录布局和 index.json 的读写。

    目录结构:
        <BACKUP_DIR>/
            index.json            # 所有备份与快照的索引
            backups/<bid>/
                manifest.json     # 本次备份元数据
                app.db            # 或 app.db.gz
            snapshots/<sid>/
                snapshot.json     # 增量快照 JSON patch
    """

    def __init__(self, root: Path = BACKUP_DIR):
        self.root = Path(root)
        self.backups_dir = self.root / BACKUP_DIRNAME
        self.snapshots_dir = self.root / SNAPSHOT_DIRNAME
        self.index_path = self.root / INDEX_NAME
        ensure_dir(self.backups_dir)
        ensure_dir(self.snapshots_dir)
        if not self.index_path.exists():
            self._write_index({"backups": OrderedDict(), "snapshots": OrderedDict()})

    # ---- index ----
    def _read_index(self) -> Dict[str, Any]:
        with open(self.index_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _write_index(self, data: Dict[str, Any]) -> None:
        with open(self.index_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def index(self) -> Dict[str, Any]:
        return self._read_index()

    # ---- backups ----
    def new_backup_id(self) -> str:
        return f"bk_{now_str()}_{uuid.uuid4().hex[:6]}"

    def backup_dir(self, bid: str) -> Path:
        p = self.backups_dir / bid
        ensure_dir(p)
        return p

    def add_backup(self, bid: str, manifest: Dict[str, Any]) -> None:
        idx = self._read_index()
        idx["backups"][bid] = {
            "id": bid,
            "created_at": now_iso(),
            "manifest_path": str((self.backups_dir / bid / META_FILE).relative_to(self.root)),
            "comment": manifest.get("comment", ""),
        }
        self._write_index(idx)

    def get_backup(self, bid: str) -> Optional[Dict[str, Any]]:
        return self._read_index()["backups"].get(bid)

    def load_backup_manifest(self, bid: str) -> Dict[str, Any]:
        entry = self.get_backup(bid)
        if not entry:
            raise KeyError(f"backup {bid} 不存在")
        with open(self.root / entry["manifest_path"], "r", encoding="utf-8") as f:
            return json.load(f)

    def latest_backup_id(self, db_name: Optional[str] = None) -> Optional[str]:
        idx = self._read_index()
        # 从新到旧遍历，优先匹配 db_name
        for bid in reversed(list(idx["backups"].keys())):
            if db_name is None:
                return bid
            try:
                m = self.load_backup_manifest(bid)
            except Exception:
                continue
            if m.get("original_name") == db_name:
                return bid
        return None

    # ---- snapshots ----
    def new_snapshot_id(self) -> str:
        return f"sn_{now_str()}_{uuid.uuid4().hex[:6]}"

    def snapshot_dir(self, sid: str) -> Path:
        p = self.snapshots_dir / sid
        ensure_dir(p)
        return p

    def add_snapshot(self, sid: str, meta: Dict[str, Any]) -> None:
        idx = self._read_index()
        idx["snapshots"][sid] = {
            "id": sid,
            "created_at": now_iso(),
            "parent": meta.get("parent"),
            "base_backup": meta.get("base_backup"),
            "db_name": meta.get("db_name"),
            "snapshot_path": str((self.snapshots_dir / sid / "snapshot.json").relative_to(self.root)),
        }
        self._write_index(idx)

    def latest_snapshot_id(self, db_name: Optional[str] = None) -> Optional[str]:
        idx = self._read_index()
        for sid in reversed(list(idx["snapshots"].keys())):
            meta = idx["snapshots"][sid]
            if db_name is None or meta.get("db_name") == db_name:
                return sid
        return None

    def load_snapshot(self, sid: str) -> Dict[str, Any]:
        entry = self._read_index()["snapshots"].get(sid)
        if not entry:
            raise KeyError(f"snapshot {sid} 不存在")
        with open(self.root / entry["snapshot_path"], "r", encoding="utf-8") as f:
            return json.load(f)


# --------------------------- SQLite 结构与数据工具 --------------------------- #

def list_tables(conn: sqlite3.Connection) -> List[str]:
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' "
        "AND name NOT LIKE 'sqlite_%' ORDER BY name"
    )
    return [r[0] for r in cur.fetchall()]


def get_table_schema(conn: sqlite3.Connection, table: str) -> Dict[str, Any]:
    """返回 {columns: [...], primary_keys: [...], create_sql: str}。"""
    cur = conn.execute(f"PRAGMA table_info('{table}')")
    columns = []
    pks = []
    for row in cur.fetchall():
        cid, name, ctype, notnull, default, pk = row
        columns.append({
            "cid": cid, "name": name, "type": ctype,
            "notnull": bool(notnull), "default": default, "pk": int(pk or 0),
        })
        if pk:
            pks.append((int(pk), name))
    pks = [n for _, n in sorted(pks)]
    create_row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    return {
        "columns": columns,
        "primary_keys": pks,
        "create_sql": create_row[0] if create_row else None,
    }


def pragma_page_size(conn: sqlite3.Connection) -> int:
    return conn.execute("PRAGMA page_size").fetchone()[0]


def pragma_page_count(conn: sqlite3.Connection) -> int:
    return conn.execute("PRAGMA page_count").fetchone()[0]


def pragma_integrity_check(conn: sqlite3.Connection) -> List[str]:
    return [r[0] for r in conn.execute("PRAGMA integrity_check").fetchall()
            if r[0] != "ok"]


def count_rows(conn: sqlite3.Connection, table: str) -> int:
    return conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]


def get_db_stats(conn: sqlite3.Connection) -> Dict[str, Any]:
    tables = list_tables(conn)
    row_counts = {t: count_rows(conn, t) for t in tables}
    return {
        "page_size": pragma_page_size(conn),
        "page_count": pragma_page_count(conn),
        "table_count": len(tables),
        "tables": tables,
        "row_counts": row_counts,
        "total_rows": sum(row_counts.values()),
    }


def table_key_cols(conn: sqlite3.Connection, table: str) -> List[str]:
    """返回用于唯一识别行的列：优先主键，否则 rowid。"""
    schema = get_table_schema(conn, table)
    if schema["primary_keys"]:
        return schema["primary_keys"]
    # WITHOUT ROWID 表且无主键？退回所有列
    cur = conn.execute(f'SELECT COUNT(*) FROM "{table}"')
    # 尝试查 rowid 是否可用
    try:
        conn.execute(f'SELECT rowid FROM "{table}" LIMIT 1')
        return ["rowid"]
    except sqlite3.OperationalError:
        return [c["name"] for c in schema["columns"]]


def fetch_all_rows(conn: sqlite3.Connection, table: str, key_cols: List[str]) -> "OrderedDict[Tuple, Dict]":
    """读取全表，以 key_cols 组合值为键，返回 字段名->值 的有序字典。"""
    schema = get_table_schema(conn, table)
    col_names = [c["name"] for c in schema["columns"]]
    quoted_cols = ",".join(f'"{c}"' for c in col_names)

    if key_cols == ["rowid"]:
        sql = f'SELECT rowid, {quoted_cols} FROM "{table}"'
        cur = conn.execute(sql)
        out: "OrderedDict[Tuple, Dict]" = OrderedDict()
        for row in cur.fetchall():
            key = (row[0],)
            values = dict(zip(col_names, row[1:]))
            out[key] = values
        return out

    quoted_keys = ",".join(f'"{c}"' for c in key_cols)
    sql = f'SELECT {quoted_keys}, {quoted_cols} FROM "{table}"'
    cur = conn.execute(sql)
    out = OrderedDict()
    klen = len(key_cols)
    for row in cur.fetchall():
        key = tuple(row[:klen])
        values = dict(zip(col_names, row[klen:]))
        out[key] = values
    return out


# ------------------------------- JSON 工具 ------------------------------- #

def _jsonable(v: Any) -> Any:
    """把 sqlite 返回值转成 JSON 可序列化值。"""
    if isinstance(v, (bytes, bytearray)):
        return {"__bytes__": v.hex()}
    if isinstance(v, (dt.datetime, dt.date, dt.time)):
        return v.isoformat()
    if isinstance(v, float) and (v != v or v in (float("inf"), float("-inf"))):
        return {"__special__": str(v)}
    return v


def _restore_jsonable(v: Any) -> Any:
    if isinstance(v, dict) and len(v) == 1:
        if "__bytes__" in v:
            return bytes.fromhex(v["__bytes__"])
        if "__special__" in v:
            s = v["__special__"]
            if s == "nan":
                return float("nan")
            if s == "inf":
                return float("inf")
            if s == "-inf":
                return float("-inf")
    if isinstance(v, list):
        return [_restore_jsonable(x) for x in v]
    if isinstance(v, dict):
        return {k: _restore_jsonable(x) for k, x in v.items()}
    return v


def json_dump(obj: Any, path: Path) -> None:
    def default(o):
        return _jsonable(o)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2, default=default)


def json_load(path: Path) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return _restore_jsonable(json.load(f))


# ================================== 命令实现 ================================== #


# ------------------------------- init-demo ------------------------------- #

def cmd_init_demo(args: argparse.Namespace) -> int:
    db_path = Path(args.db)
    if db_path.exists() and not args.force:
        print(f"[!] {db_path} 已存在，使用 --force 覆盖", file=sys.stderr)
        return 2
    if db_path.exists():
        db_path.unlink()

    with closing(sqlite3.connect(db_path)) as conn:
        conn.executescript("""
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            username TEXT NOT NULL UNIQUE,
            email    TEXT NOT NULL,
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE orders (
            id          INTEGER PRIMARY KEY,
            user_id     INTEGER NOT NULL REFERENCES users(id),
            amount      REAL    NOT NULL,
            status      TEXT    NOT NULL DEFAULT 'pending',
            placed_at   TEXT    DEFAULT (datetime('now'))
        );

        CREATE TABLE logs (
            id          INTEGER PRIMARY KEY,
            level       TEXT NOT NULL DEFAULT 'INFO',
            message     TEXT NOT NULL,
            context     TEXT,
            logged_at   TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX idx_orders_user ON orders(user_id);
        CREATE INDEX idx_logs_level ON logs(level);
        """)
        users = [
            ("alice",   "alice@example.com"),
            ("bob",     "bob@example.com"),
            ("carol",   "carol@example.com"),
            ("dave",    "dave@example.com"),
            ("eve",     "eve@example.com"),
        ]
        conn.executemany(
            "INSERT INTO users(username,email) VALUES(?,?)", users
        )
        # 每个用户若干订单
        orders = []
        for uid in range(1, len(users) + 1):
            for i in range(uid % 3 + 1):
                orders.append((uid, round(10 + uid * 5.3 + i * 12.7, 2),
                               ["pending", "paid", "shipped"][(uid + i) % 3]))
        conn.executemany(
            "INSERT INTO orders(user_id,amount,status) VALUES(?,?,?)", orders
        )
        # 若干日志
        levels = ["DEBUG", "INFO", "WARN", "ERROR"]
        logs = [
            (levels[i % 4], f"事件 #{i} 发生",
             json.dumps({"source": f"module_{i % 5}", "seq": i}, ensure_ascii=False))
            for i in range(1, 31)
        ]
        conn.executemany(
            "INSERT INTO logs(level,message,context) VALUES(?,?,?)", logs
        )
        conn.commit()

    stats = _inspect_db_file(db_path)
    print(f"[✓] 示例数据库已生成: {db_path}")
    print(f"    表数量: {stats['table_count']}  总行数: {stats['total_rows']}  "
          f"页大小: {stats['page_size']}B  大小: {db_path.stat().st_size} bytes")
    return 0


def _inspect_db_file(path: Path) -> Dict[str, Any]:
    with closing(connect_readonly(path)) as conn:
        return get_db_stats(conn)


# -------------------------------- backup -------------------------------- #

def cmd_backup(args: argparse.Namespace) -> int:
    src = Path(args.db)
    if not safe_exists_db(src):
        print(f"[!] {src} 不存在或不是有效的 SQLite 文件", file=sys.stderr)
        return 2

    store = Store(Path(args.store))
    bid = store.new_backup_id()
    bdir = store.backup_dir(bid)

    # 1) 复制文件
    use_gzip = args.gzip
    dst_name = src.name + (".gz" if use_gzip else "")
    dst_file = bdir / dst_name

    # 使用 sqlite backup API 复制，确保原子性
    with tempfile.NamedTemporaryFile(delete=False, suffix=src.suffix or ".db") as tmp:
        tmp_path = Path(tmp.name)
    try:
        with closing(connect_readonly(src)) as src_conn, \
             closing(sqlite3.connect(tmp_path)) as dst_conn:
            src_conn.backup(dst_conn)
            dst_conn.commit()
        # 采集元数据（使用临时副本，避免源被修改）
        with closing(connect_readonly(tmp_path)) as meta_conn:
            stats = get_db_stats(meta_conn)
            integrity = pragma_integrity_check(meta_conn)
        if use_gzip:
            gzip_compress(tmp_path, dst_file)
        else:
            shutil.copy2(tmp_path, dst_file)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()

    sha = sha256_file(dst_file)
    size = dst_file.stat().st_size

    manifest = {
        "id": bid,
        "created_at": now_iso(),
        "original_path": str(src.resolve()),
        "original_name": src.name,
        "file_name": dst_name,
        "file_size": size,
        "sha256": sha,
        "compressed": use_gzip,
        "comment": args.comment or "",
        "stats": stats,
        "integrity_check_ok": len(integrity) == 0,
        "integrity_messages": integrity,
    }
    json_dump(manifest, bdir / META_FILE)
    store.add_backup(bid, manifest)

    print(f"[✓] 备份完成 -> {bid}")
    print(f"    文件: {dst_file} ({_human_size(size)})")
    print(f"    SHA256: {sha}")
    print(f"    表:{stats['table_count']}  行:{stats['total_rows']}  "
          f"页大小:{stats['page_size']}B  完整性:{'OK' if manifest['integrity_check_ok'] else 'FAIL'}")
    if args.comment:
        print(f"    备注: {args.comment}")
    return 0


def _human_size(n: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    v = float(n)
    while v >= 1024 and i < len(units) - 1:
        v /= 1024
        i += 1
    return f"{v:.2f} {units[i]}"


# ------------------------------- snapshot ------------------------------- #

def cmd_snapshot(args: argparse.Namespace) -> int:
    db_path = Path(args.db)
    if not safe_exists_db(db_path):
        print(f"[!] {db_path} 不存在或不是有效的 SQLite 文件", file=sys.stderr)
        return 2

    store = Store(Path(args.store))
    sid = store.new_snapshot_id()
    sdir = store.snapshot_dir(sid)

    ignore = set(args.ignore or [])

    # 确定 parent: 优先 --parent，其次该 db 的最新 snapshot，再其次对应 db 的最新 backup
    parent_sid: Optional[str] = None
    parent_type: str = "backup"  # or "snapshot"
    base_backup_id: Optional[str] = None

    if args.parent:
        if args.parent.startswith("sn_"):
            parent_sid = args.parent
            parent_type = "snapshot"
            base_backup_id = store.load_snapshot(parent_sid).get("base_backup")
        elif args.parent.startswith("bk_"):
            # 从 backup 开始新快照链
            base_backup_id = args.parent
        else:
            print(f"[!] --parent 必须是 sn_... 或 bk_...: {args.parent}", file=sys.stderr)
            return 2
    else:
        latest_sn = store.latest_snapshot_id(db_name=db_path.name)
        if latest_sn:
            parent_sid = latest_sn
            parent_type = "snapshot"
            base_backup_id = store.load_snapshot(latest_sn).get("base_backup")
        else:
            latest_bk = store.latest_backup_id(db_name=db_path.name)
            if latest_bk:
                base_backup_id = latest_bk
            else:
                print(f"[!] 找不到 {db_path.name} 的备份或快照作为基线，请先 backup 或使用 --parent",
                      file=sys.stderr)
                return 2

    # 把 parent（backup 或 快照所基的状态）展开成一个临时 db 作为对比基线
    with tempfile.TemporaryDirectory() as td:
        td_path = Path(td)
        base_db = td_path / "base.db"
        if parent_type == "backup":
            _extract_backup_to(store, base_backup_id, base_db)
        else:
            # 先展开 base_backup 再按顺序 apply 所有历史快照
            chain = _snapshot_chain(store, parent_sid)
            _extract_backup_to(store, base_backup_id, base_db)
            for psid in chain:
                _apply_snapshot_file(store.load_snapshot(psid), base_db, dry_run=False)

        # 对比当前 db 与 base_db
        snapshot_payload = _diff_databases(
            base_db, db_path, db_path.name, ignore_tables=ignore
        )

    snapshot_payload.update({
        "id": sid,
        "created_at": now_iso(),
        "parent": parent_sid or base_backup_id,
        "parent_type": parent_type,
        "base_backup": base_backup_id,
        "db_name": db_path.name,
        "ignored_tables": sorted(ignore),
    })
    snap_file = sdir / "snapshot.json"
    json_dump(snapshot_payload, snap_file)
    store.add_snapshot(sid, snapshot_payload)

    summary = snapshot_payload["summary"]
    print(f"[✓] 快照生成 -> {sid}  (parent: {snapshot_payload['parent']})")
    print(f"    新增表: {len(summary['added_tables'])}  "
          f"删除表: {len(summary['removed_tables'])}  "
          f"结构变化表: {len(summary['schema_changed_tables'])}")
    print(f"    新增行: {summary['added_rows']}  "
          f"删除行: {summary['removed_rows']}  "
          f"修改行: {summary['modified_rows']}")
    return 0


def _snapshot_chain(store: Store, sid: str) -> List[str]:
    """返回从 base_backup 之后到 sid（含）按先后顺序的快照 id 列表。"""
    chain = []
    cur = sid
    visited = set()
    while True:
        if cur in visited:
            raise RuntimeError(f"快照链存在环: {cur}")
        visited.add(cur)
        chain.append(cur)
        meta = store._read_index()["snapshots"].get(cur)
        if not meta:
            break
        parent = meta.get("parent")
        if not parent or parent.startswith("bk_"):
            break
        cur = parent
    return list(reversed(chain))


def _extract_backup_to(store: Store, bid: str, dest: Path) -> None:
    m = store.load_backup_manifest(bid)
    backup_file = store.root / store.get_backup(bid)["manifest_path"]
    backup_file = backup_file.parent / m["file_name"]
    if m.get("compressed"):
        gzip_decompress(backup_file, dest)
    else:
        shutil.copy2(backup_file, dest)
    if not safe_exists_db(dest):
        raise RuntimeError(f"从备份 {bid} 还原失败：目标不是有效的 sqlite 文件")


def _diff_databases(old_db: Path, new_db: Path, db_name: str,
                    ignore_tables: Iterable[str] = ()) -> Dict[str, Any]:
    """返回两张 db 的 diff（结构 + 行级 JSON patch）。"""
    ignore = set(ignore_tables)
    with closing(connect_readonly(old_db)) as o, \
         closing(connect_readonly(new_db)) as n:
        o_tables = set(list_tables(o)) - ignore
        n_tables = set(list_tables(n)) - ignore
        added_tables = sorted(n_tables - o_tables)
        removed_tables = sorted(o_tables - n_tables)
        common = sorted(o_tables & n_tables)

        table_patches: Dict[str, Any] = OrderedDict()
        schema_changed: List[str] = []
        added_rows = removed_rows = modified_rows = 0

        for t in common:
            old_schema = get_table_schema(o, t)
            new_schema = get_table_schema(n, t)
            schema_diff = _schema_diff(old_schema, new_schema)
            patch_entry: Dict[str, Any] = {"schema": schema_diff}
            if schema_diff["changed"]:
                schema_changed.append(t)

            # 只在列结构“兼容”（旧列集是新列集子集或反之也大致可比）时做行对比
            old_cols = {c["name"] for c in old_schema["columns"]}
            new_cols = {c["name"] for c in new_schema["columns"]}
            comparable = old_cols & new_cols

            # 选 key：优先新主键，若新主键列在旧表不全有 → 尝试旧主键
            old_schema_cols = {c["name"] for c in old_schema["columns"]}
            new_schema_cols = {c["name"] for c in new_schema["columns"]}

            key_cols = table_key_cols(n, t)
            all_in_old = all(k in old_schema_cols for k in key_cols)

            if not all_in_old:
                # 新主键在旧表不全有 → 尝试旧主键
                old_key_cols = table_key_cols(o, t)
                all_in_new = all(k in new_schema_cols for k in old_key_cols)
                if all_in_new:
                    key_cols = old_key_cols
                else:
                    # 无法对齐，只记结构差异
                    patch_entry["rows"] = {"skipped": True, "reason": "key 列在两库中不对齐"}
                    table_patches[t] = patch_entry
                    continue

            old_rows = fetch_all_rows(o, t, key_cols)
            new_rows = fetch_all_rows(n, t, key_cols)
            new_only_cols = sorted(new_cols - old_cols)
            rows_patch = _row_patch(old_rows, new_rows, comparable, key_cols,
                                    new_only_cols=new_only_cols)
            patch_entry["rows"] = rows_patch
            added_rows += rows_patch["added_count"]
            removed_rows += rows_patch["removed_count"]
            modified_rows += rows_patch["modified_count"]
            table_patches[t] = patch_entry

        # 新增表：记录全量行（added）
        for t in added_tables:
            key_cols = table_key_cols(n, t)
            new_rows = fetch_all_rows(n, t, key_cols)
            added_rows += len(new_rows)
            schema = get_table_schema(n, t)
            table_patches[t] = {
                "schema": {"changed": True, "added": schema["columns"],
                           "removed": [], "modified": []},
                "rows": {
                    "skipped": False,
                    "added": [{"key": list(k), "values": v} for k, v in new_rows.items()],
                    "removed": [],
                    "modified": [],
                    "added_count": len(new_rows),
                    "removed_count": 0,
                    "modified_count": 0,
                },
            }

        # 删除表：记录原行（removed）
        for t in removed_tables:
            try:
                key_cols = table_key_cols(o, t)
                old_rows = fetch_all_rows(o, t, key_cols)
            except Exception:
                old_rows = OrderedDict()
                key_cols = []
            removed_rows += len(old_rows)
            schema = get_table_schema(o, t)
            table_patches[t] = {
                "schema": {"changed": True, "added": [],
                           "removed": schema["columns"], "modified": []},
                "rows": {
                    "skipped": False,
                    "added": [],
                    "removed": [{"key": list(k), "values": v} for k, v in old_rows.items()],
                    "modified": [],
                    "added_count": 0,
                    "removed_count": len(old_rows),
                    "modified_count": 0,
                },
            }

    return {
        "db_name": db_name,
        "added_tables": added_tables,
        "removed_tables": removed_tables,
        "schema_changed_tables": sorted(schema_changed),
        "summary": {
            "added_tables": added_tables,
            "removed_tables": removed_tables,
            "schema_changed_tables": sorted(schema_changed),
            "added_rows": added_rows,
            "removed_rows": removed_rows,
            "modified_rows": modified_rows,
        },
        "tables": table_patches,
    }


def _schema_diff(old: Dict[str, Any], new: Dict[str, Any]) -> Dict[str, Any]:
    oc = {c["name"]: c for c in old["columns"]}
    nc = {c["name"]: c for c in new["columns"]}
    added = [nc[n] for n in sorted(set(nc) - set(oc))]
    removed = [oc[n] for n in sorted(set(oc) - set(nc))]
    modified = []
    for name in sorted(set(oc) & set(nc)):
        a, b = oc[name], nc[name]
        diffs = {}
        for k in ("type", "notnull", "default", "pk"):
            if a.get(k) != b.get(k):
                diffs[k] = {"old": a.get(k), "new": b.get(k)}
        if diffs:
            modified.append({"name": name, "changes": diffs})
    changed = bool(added or removed or modified
                   or old["primary_keys"] != new["primary_keys"])
    return {
        "changed": changed,
        "primary_keys_old": old["primary_keys"],
        "primary_keys_new": new["primary_keys"],
        "added": added,
        "removed": removed,
        "modified": modified,
        "new_columns": new["columns"],
        "old_columns": old["columns"],
        "new_create_sql": new.get("create_sql"),
        "old_create_sql": old.get("create_sql"),
    }


def _row_patch(old: "OrderedDict[Tuple, Dict]", new: "OrderedDict[Tuple, Dict]",
               comparable: set, key_cols: List[str],
               new_only_cols: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    生成行级 patch。
    comparable: 两表共有的列名集合
    new_only_cols: 仅新表有、旧表没有的列（结构新增列）。
                    如果 new_only_cols 非空，所有公共行都会出现在 modified 中，
    里包含这些新列的值（old=None 表示从无到有）。
    """
    added_keys = set(new) - set(old)
    removed_keys = set(old) - set(new)
    common_keys = set(old) & set(new)
    new_only = new_only_cols or []
    added = []
    removed = []
    modified = []
    for k in sorted(added_keys, key=lambda x: (0, x)):
        added.append({"key": list(k), "values": new[k]})
    for k in sorted(removed_keys, key=lambda x: (0, x)):
        removed.append({"key": list(k), "values": old[k]})
    for k in sorted(common_keys, key=lambda x: (0, x)):
        ov, nv = old[k], new[k]
        field_diff: Dict[str, Any] = {}
        for col in sorted(comparable):
            a, b = ov.get(col), nv.get(col)
            if _jsonable(a) != _jsonable(b):
                field_diff[col] = {"old": _jsonable(a), "new": _jsonable(b)}
        # 新增列：所有公共行都记录新值（old 侧不存在，记为 null）
        for col in sorted(new_only):
            field_diff[col] = {"old": None, "new": _jsonable(nv.get(col))}
        if field_diff:
            modified.append({"key": list(k), "fields": field_diff})
    return {
        "skipped": False,
        "key_columns": key_cols,
        "new_only_columns": new_only,
        "added": added,
        "removed": removed,
        "modified": modified,
        "added_count": len(added),
        "removed_count": len(removed),
        "modified_count": len(modified),
    }


# ------------------------------- restore ------------------------------- #

def cmd_restore(args: argparse.Namespace) -> int:
    store = Store(Path(args.store))

    # 定位 backup id
    bid = args.backup
    if os.sep in bid or bid.endswith((".db", ".gz")):
        # 用户传的是路径
        backup_path = Path(bid)
        if not backup_path.exists():
            print(f"[!] 备份文件不存在: {backup_path}", file=sys.stderr)
            return 2
        bid = _bid_from_standalone_backup(backup_path)
        manifest = _standalone_manifest(backup_path, bid)
    else:
        if store.get_backup(bid) is None:
            print(f"[!] 备份 {bid} 不存在，可运行 history 查看", file=sys.stderr)
            return 2
        manifest = store.load_backup_manifest(bid)
        backup_path = store.root / store.get_backup(bid)["manifest_path"]
        backup_path = backup_path.parent / manifest["file_name"]

    out_path = Path(args.output) if args.output else Path(manifest["original_name"])

    # dry-run
    if args.dry_run:
        print(f"[dry-run] 将要从备份 {bid} 恢复到 {out_path}")
        stats = manifest.get("stats", {})
        print(f"    表数量: {stats.get('table_count','?')}  "
              f"总行数: {stats.get('total_rows','?')}")
        for t, c in stats.get("row_counts", {}).items():
            print(f"      - {t}: {c} 行")
        if out_path.exists():
            safe = _safe_copy_name(out_path)
            print(f"    * 将先创建安全副本: {safe}")
        return 0

    # 安全副本
    if out_path.exists():
        safe = _safe_copy_name(out_path)
        try:
            shutil.copy2(out_path, safe)
            print(f"[i] 已创建安全副本: {safe}")
        except OSError as e:
            print(f"[!] 无法创建安全副本: {e}", file=sys.stderr)
            if not args.force:
                return 3

    # 还原
    with tempfile.TemporaryDirectory() as td:
        tmp_db = Path(td) / "restored.db"
        if manifest.get("compressed") or is_gzip(backup_path):
            gzip_decompress(backup_path, tmp_db)
        else:
            shutil.copy2(backup_path, tmp_db)
        if not safe_exists_db(tmp_db):
            print(f"[!] 备份内容不是有效的 SQLite 文件", file=sys.stderr)
            return 4
        # 校验哈希（仅当有 manifest 时）
        if "sha256" in manifest:
            # manifest 记录的是压缩/未压缩文件本身的 sha
            actual = sha256_file(backup_path)
            if actual != manifest["sha256"]:
                print(f"[!] SHA256 校验失败，期望 {manifest['sha256']}，实际 {actual}",
                      file=sys.stderr)
                if not args.force:
                    return 5
            else:
                print(f"[i] SHA256 校验通过")
        # 完整性
        with closing(connect_readonly(tmp_db)) as conn:
            bad = pragma_integrity_check(conn)
        if bad:
            print(f"[!] integrity_check 未通过: {bad}", file=sys.stderr)
            if not args.force:
                return 6
        ensure_dir(out_path.parent) if out_path.parent != Path() else None
        shutil.copy2(tmp_db, out_path)

    print(f"[✓] 已恢复到 {out_path}")
    return 0


def _safe_copy_name(p: Path) -> Path:
    ts = now_str()
    return p.with_name(p.name + SAFE_COPY_SUFFIX + ts)


def _standalone_manifest(backup_file: Path, bid: str) -> Dict[str, Any]:
    """当用户直接给路径时构造临时 manifest。"""
    compressed = is_gzip(backup_file)
    sha = sha256_file(backup_file)
    stats: Dict[str, Any] = {}
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td) / "x.db"
        if compressed:
            gzip_decompress(backup_file, tmp)
        else:
            shutil.copy2(backup_file, tmp)
        if safe_exists_db(tmp):
            with closing(connect_readonly(tmp)) as conn:
                stats = get_db_stats(conn)
    return {
        "id": bid,
        "file_name": backup_file.name,
        "original_name": re.sub(r"\.gz$", "", backup_file.name),
        "compressed": compressed,
        "sha256": sha,
        "stats": stats,
    }


def _bid_from_standalone_backup(path: Path) -> str:
    return f"bk_adhoc_{path.stem}_{sha256_file(path)[:8]}"


# --------------------------------- apply --------------------------------- #

def cmd_apply(args: argparse.Namespace) -> int:
    store = Store(Path(args.store))
    sid = args.snapshot
    if os.sep in sid or sid.endswith(".json"):
        snap_path = Path(sid)
        if not snap_path.exists():
            print(f"[!] 快照文件不存在: {snap_path}", file=sys.stderr)
            return 2
        payload = json_load(snap_path)
    else:
        if store._read_index()["snapshots"].get(sid) is None:
            print(f"[!] 快照 {sid} 不存在，可运行 history 查看", file=sys.stderr)
            return 2
        payload = store.load_snapshot(sid)

    target = Path(args.target)
    if not target.exists():
        print(f"[!] 目标数据库不存在: {target}", file=sys.stderr)
        return 2

    # dry-run
    if args.dry_run:
        plan = _apply_snapshot_file(payload, target, dry_run=True)
        print(f"[dry-run] 将应用快照 {payload.get('id', sid)} 到 {target}")
        print(f"    新增表: {len(plan['added_tables'])}  "
              f"删除表: {len(plan['removed_tables'])}  "
              f"结构变化: {len(plan['schema_changed'])}")
        print(f"    新增行: {plan['added_rows']}  "
              f"删除行: {plan['removed_rows']}  "
              f"修改行: {plan['modified_rows']}")
        return 0

    # 安全副本
    safe = _safe_copy_name(target)
    shutil.copy2(target, safe)
    print(f"[i] 已创建安全副本: {safe}")

    result = _apply_snapshot_file(payload, target, dry_run=False)
    print(f"[✓] 快照已应用到 {target}")
    print(f"    新增表:{len(result['added_tables'])}  "
          f"删除表:{len(result['removed_tables'])}  "
          f"结构变化:{len(result['schema_changed'])}")
    print(f"    新增行:{result['added_rows']}  "
          f"删除行:{result['removed_rows']}  "
          f"修改行:{result['modified_rows']}")
    return 0


def _apply_snapshot_file(payload: Dict[str, Any], target: Path,
                         dry_run: bool) -> Dict[str, Any]:
    """将一个 snapshot.json 的内容应用到 target；dry_run 仅返回计划。"""
    plan: Dict[str, Any] = {
        "added_tables": payload.get("added_tables", []),
        "removed_tables": payload.get("removed_tables", []),
        "schema_changed": payload.get("schema_changed_tables", []),
        "added_rows": 0, "removed_rows": 0, "modified_rows": 0,
    }

    if dry_run:
        for t, patch in payload.get("tables", {}).items():
            r = patch.get("rows", {})
            plan["added_rows"] += r.get("added_count", 0)
            plan["removed_rows"] += r.get("removed_count", 0)
            plan["modified_rows"] += r.get("modified_count", 0)
        return plan

    with closing(sqlite3.connect(target)) as conn:
        conn.execute("PRAGMA foreign_keys = OFF")
        for t, patch in payload.get("tables", {}).items():
            schema = patch.get("schema", {})
            rows = patch.get("rows", {})
            # 处理表级：删除 / 新建 / 修改列
            if t in payload.get("removed_tables", []):
                conn.execute(f'DROP TABLE IF EXISTS "{t}"')
                plan["removed_rows"] += rows.get("removed_count", 0)
                continue
            if t in payload.get("added_tables", []):
                # 从 added 列推导建表
                cols_sql = []
                pks = []
                for c in schema.get("added", []):
                    s = f'"{c["name"]}" {c["type"] or "TEXT"}'
                    if c.get("notnull"):
                        s += " NOT NULL"
                    if c.get("default") is not None:
                        s += f" DEFAULT {_normalize_default(c['default'])}"
                    if c.get("pk"):
                        pks.append((c["pk"], c["name"]))
                    cols_sql.append(s)
                if pks:
                    pk_names = [n for _, n in sorted(pks)]
                    cols_sql.append(f"PRIMARY KEY ({','.join('\"'+x+'\"' for x in pk_names)})")
                ddl = f'CREATE TABLE IF NOT EXISTS "{t}" ({", ".join(cols_sql)})'
                conn.execute(ddl)
                _insert_rows(conn, t, rows.get("added", []))
                plan["added_rows"] += rows.get("added_count", 0)
                continue

            # 公共表
            if schema.get("changed"):
                # 有结构变化：分三阶段
                # 0) 先保存旧索引（loose 重建会丢索引）
                old_indexes = _get_table_indexes(conn, t)
                # 1) 宽松版重建：所有列可空、无主键约束，先把数据迁过去
                _rebuild_table_loose(conn, t, schema)
                # 2) 应用行 patch（此时新增列会被填上精确值）
                if not rows.get("skipped"):
                    r = _apply_row_patch(conn, t, rows)
                    plan["added_rows"] += r["added"]
                    plan["removed_rows"] += r["removed"]
                    plan["modified_rows"] += r["modified"]
                # 3) 严格版重建：加回 NOT NULL、主键约束，重建索引
                _rebuild_table_strict(conn, t, schema, old_indexes)
            else:
                # 无结构变化，直接应用行变更
                if not rows.get("skipped"):
                    r = _apply_row_patch(conn, t, rows)
                    plan["added_rows"] += r["added"]
                    plan["removed_rows"] += r["removed"]
                    plan["modified_rows"] += r["modified"]
        conn.commit()
        conn.execute("PRAGMA foreign_keys = ON")
    return plan


def _insert_rows(conn: sqlite3.Connection, table: str, rows: List[Dict]) -> int:
    if not rows:
        return 0
    sample = rows[0]["values"]
    cols = list(sample.keys())
    qmarks = ",".join(["?"] * len(cols))
    col_list = ",".join(f'"{c}"' for c in cols)
    sql = f'INSERT INTO "{table}" ({col_list}) VALUES ({qmarks})'
    batch = []
    for r in rows:
        vals = [_py_to_sql(r["values"].get(c)) for c in cols]
        batch.append(tuple(vals))
    conn.executemany(sql, batch)
    return len(batch)


def _py_to_sql(v: Any) -> Any:
    if isinstance(v, dict) and "__bytes__" in v:
        return bytes.fromhex(v["__bytes__"])
    return v


def _normalize_default(raw: Any) -> str:
    """
    PRAGMA table_info 返回的 default 值规则：
      - 对于字面量 (字符串/数字)：外层被双引号包裹，如 "'hello'" , "123"
      - 对于表达式 (如 datetime('now'))：外层也被双引号包裹，如 "(datetime('now'))" 或 "datetime('now')"
    去掉最外层的双引号（如果存在），就得到合法的 SQL DEFAULT 片段。
    """
    if raw is None:
        return "NULL"
    s = str(raw)
    if len(s) >= 2 and s[0] == '"' and s[-1] == '"':
        s = s[1:-1]
    # 如果看起来是表达式（非字面量）且没有外层括号，给它包一层，兼容老版本 SQLite
    if s and s[0] not in ("'", '"') and not s[0].isdigit() \
       and s.upper() not in ("NULL", "TRUE", "FALSE") \
       and not (s.startswith("(") and s.endswith(")")):
        # 比如 datetime('now') -> (datetime('now'))
        return f"({s})"
    return s


def _rebuild_table_loose(conn: sqlite3.Connection, table: str, schema: Dict[str, Any]) -> None:
    """
    第一阶段重建：将旧表改造成「宽松版」新结构。
    宽松 = 所有列可空、无主键约束。目的：先把列对齐，允许迁移数据。
    """
    # 读取旧列（用于数据迁移）
    cur = conn.execute(f'PRAGMA table_info("{table}")')
    old_cols = [{"name": r[1], "type": r[2], "notnull": bool(r[3]),
                 "default": r[4], "pk": int(r[5] or 0)} for r in cur.fetchall()]
    old_col_names = {c["name"] for c in old_cols}

    # 目标新列（完整定义）
    new_columns = schema["new_columns"]
    new_col_names = {c["name"] for c in new_columns}

    # 仅新增列，无删除/修改/主键变化 → 用 ALTER TABLE ADD 更快
    only_add = (not schema.get("removed")) and (not schema.get("modified")) \
               and schema.get("primary_keys_old") == schema.get("primary_keys_new")
    if only_add:
        for c in schema.get("added", []):
            sql = f'ALTER TABLE "{table}" ADD COLUMN "{c["name"]}" {c["type"] or "TEXT"}'
            try:
                conn.execute(sql)
            except sqlite3.OperationalError as e:
                print(f"    [!] 无法添加列 {c['name']} 到 {table}: {e}", file=sys.stderr)
        return

    # 需要重建：宽松版（所有列可空，无主键）
    loose_cols = [dict(c, notnull=False, pk=0) for c in new_columns]
    common = [c["name"] for c in new_columns if c["name"] in old_col_names]

    _do_rebuild_table(conn, table, loose_cols, pks=[], keep_data_cols=common)


def _rebuild_table_strict(conn: sqlite3.Connection, table: str, schema: Dict[str, Any],
                          old_indexes: Optional[List[Tuple[str, str]]] = None) -> None:
    """
    第二阶段重建：将宽松版表改造成「严格版」——加上 NOT NULL、主键约束，重建索引。
    如果有完整 new_create_sql，则优先使用（保留所有约束如 UNIQUE/CHECK 等）。
    """
    new_columns = schema["new_columns"]
    new_pks = schema["primary_keys_new"]
    new_create_sql = schema.get("new_create_sql")

    if old_indexes is None:
        old_indexes = _get_table_indexes(conn, table)
    cur_col_names = [c["name"] for c in new_columns]

    _do_rebuild_table(conn, table, new_columns, pks=new_pks,
                      keep_data_cols=cur_col_names, indexes=old_indexes,
                      create_sql=new_create_sql)


def _do_rebuild_table(conn: sqlite3.Connection, table: str,
                      columns: List[Dict], pks: List[str],
                      keep_data_cols: Optional[List[str]] = None,
                      indexes: Optional[List[Tuple[str, str]]] = None,
                      create_sql: Optional[str] = None) -> None:
    """
    通用表重建：用指定列/主键重建 table，迁移 keep_data_cols 的数据，最后重建索引。
    如果提供 create_sql（完整 CREATE TABLE 语句），则优先用它建表（更准确保留约束）。
    """
    if keep_data_cols is None:
        keep_data_cols = [c["name"] for c in columns]
    if indexes is None:
        indexes = []

    tmp_name = f"__tmp_rebuild_{table}"
    conn.execute(f'DROP TABLE IF EXISTS "{tmp_name}"')

    # 建新表
    if create_sql:
        # 替换表名为临时表名：CREATE TABLE "oldname" (...) → CREATE TABLE "tmp_name" (...)
        # 用正则替换第一个出现的表名
        tmp_create_sql = re.sub(
            r'(?i)CREATE\s+TABLE\s+(?:"|\')?\w+(?:"|\')?',
            f'CREATE TABLE "{tmp_name}"',
            create_sql, count=1
        )
        conn.execute(tmp_create_sql)
    else:
        ddl = _build_create_table_ddl(tmp_name, columns, pks)
        conn.execute(ddl)

    # 迁移数据
    if keep_data_cols:
        col_list = ",".join(f'"{c}"' for c in keep_data_cols)
        conn.execute(f'INSERT INTO "{tmp_name}" ({col_list}) SELECT {col_list} FROM "{table}"')

    # 替换
    conn.execute(f'DROP TABLE "{table}"')
    conn.execute(f'ALTER TABLE "{tmp_name}" RENAME TO "{table}"')

    # 重建索引
    col_names_set = {c["name"] for c in columns}
    for idx_name, idx_sql in indexes:
        idx_cols_set = _extract_index_columns(idx_sql)
        if idx_cols_set and not idx_cols_set.issubset(col_names_set):
            print(f"    [i] 索引 {idx_name} 引用已删除列，跳过重建", file=sys.stderr)
            continue
        try:
            conn.execute(idx_sql)
        except sqlite3.OperationalError as e:
            print(f"    [!] 重建索引 {idx_name} 失败: {e}", file=sys.stderr)


def _compute_new_columns(old_cols: List[Dict], schema: Dict[str, Any]) -> Tuple[List[Dict], List[str]]:
    """根据旧列 + schema patch 计算新列定义和主键。"""
    removed = {c["name"] for c in schema.get("removed", [])}
    modified = {c["name"]: c["changes"] for c in schema.get("modified", [])}
    added_list = schema.get("added", [])
    added_map = {c["name"]: c for c in added_list}
    new_pks_old = schema.get("primary_keys_old", [])
    new_pks_new = schema.get("primary_keys_new", [])

    new_cols: List[Dict] = []

    # 处理旧列（保留/修改/删除）
    for c in old_cols:
        name = c["name"]
        if name in removed:
            continue
        col = dict(c)
        if name in modified:
            changes = modified[name]
            for k, v in changes.items():
                if k == "type":
                    col["type"] = v["new"]
                elif k == "notnull":
                    col["notnull"] = v["new"]
                elif k == "default":
                    col["default"] = v["new"]
                elif k == "pk":
                    col["pk"] = v["new"]
        # 如果 added 里也有同名列（说明是"新增列在当前阶段已经存在"，以 added 的最新定义为准）
        if name in added_map:
            for k, v in added_map[name].items():
                if k == "pk":
                    col[k] = int(v or 0)
                elif k == "notnull":
                    col[k] = bool(v)
                else:
                    col[k] = v
            del added_map[name]
        new_cols.append(col)

    # 加上真正新增的列（added_map 剩下的就是旧表没有的）
    for c in added_list:
        if c["name"] in added_map:
            new_cols.append(dict(c))

    # 重排 pk 字段值，使之与 primary_keys_new 顺序一致
    pk_names = list(new_pks_new) if new_pks_new else []
    for i, pname in enumerate(pk_names, start=1):
        for c in new_cols:
            if c["name"] == pname:
                c["pk"] = i
                break
    # 不在新主键里的列，pk=0
    if pk_names:
        for c in new_cols:
            if c["name"] not in pk_names:
                c["pk"] = 0

    return new_cols, pk_names


def _build_create_table_ddl(table_name: str, columns: List[Dict], pks: List[str]) -> str:
    """根据列定义+主键列表生成 CREATE TABLE DDL。"""
    col_sqls = []
    for c in columns:
        s = f'"{c["name"]}" {c["type"] or "TEXT"}'
        if c.get("notnull"):
            s += " NOT NULL"
        if c.get("default") is not None:
            s += f" DEFAULT {_normalize_default(c['default'])}"
        # 单列主键且是 INTEGER + pk=1 时写成 PRIMARY KEY（带 autoincrement 效果）
        # 这里统一走表级 PRIMARY KEY 约束，更稳妥
        col_sqls.append(s)

    if pks:
        pk_list = ",".join(f'"{p}"' for p in pks)
        col_sqls.append(f"PRIMARY KEY ({pk_list})")

    return f'CREATE TABLE "{table_name}" ({", ".join(col_sqls)})'


def _get_table_indexes(conn: sqlite3.Connection, table: str) -> List[Tuple[str, str]]:
    """返回 [(index_name, create_sql), ...]，自动排除 sqlite_autoindex_ 开头的。"""
    rows = conn.execute(
        "SELECT name, sql FROM sqlite_master WHERE type='index' AND tbl_name=?", (table,)
    ).fetchall()
    return [(r[0], r[1]) for r in rows if r[1] and not r[0].startswith("sqlite_autoindex_")]


def _extract_index_columns(idx_sql: str) -> Optional[set]:
    """简单解析 CREATE INDEX ... ON table(col1, col2, ...) 返回列名集合。"""
    m = re.search(r"\(([^)]+)\)", idx_sql)
    if not m:
        return None
    cols = []
    for part in m.group(1).split(","):
        part = part.strip().strip('"').strip("'")
        # 去掉 ASC/DESC
        part = re.sub(r"\s+(ASC|DESC)$", "", part, flags=re.IGNORECASE).strip()
        cols.append(part)
    return set(cols)


def _apply_row_patch(conn: sqlite3.Connection, table: str, rows: Dict[str, Any]) -> Dict[str, int]:
    added = removed = modified = 0
    key_cols: List[str] = rows.get("key_columns", [])
    # 先删除再插入再更新，避免顺序问题
    for r in rows.get("removed", []):
        where_sql, params = _key_where(key_cols, r["key"])
        conn.execute(f'DELETE FROM "{table}" WHERE {where_sql}', params)
        removed += 1
    added += _insert_rows(conn, table, rows.get("added", []))
    for r in rows.get("modified", []):
        fields = r["fields"]  # {col: {old, new}}
        if not fields:
            continue
        set_sql = ",".join(f'"{c}" = ?' for c in fields.keys())
        new_vals = [_py_to_sql(v["new"]) for v in fields.values()]
        where_sql, params = _key_where(key_cols, r["key"])
        conn.execute(f'UPDATE "{table}" SET {set_sql} WHERE {where_sql}',
                     new_vals + list(params))
        modified += 1
    return {"added": added, "removed": removed, "modified": modified}


def _key_where(key_cols: List[str], key_vals: List[Any]) -> Tuple[str, List[Any]]:
    if not key_cols:
        return "1=0", []
    parts = []
    params = []
    for k, v in zip(key_cols, key_vals):
        if v is None:
            parts.append(f'"{k}" IS NULL')
        else:
            parts.append(f'"{k}" = ?')
            params.append(_py_to_sql(v))
    return " AND ".join(parts), params


# -------------------------------- verify -------------------------------- #

def cmd_verify(args: argparse.Namespace) -> int:
    store = Store(Path(args.store))
    target = args.target
    ok = True

    # 1) 校验备份
    if target.startswith("bk_"):
        if store.get_backup(target) is None:
            print(f"[!] 备份不存在: {target}", file=sys.stderr)
            return 2
        manifest = store.load_backup_manifest(target)
        backup_path = store.root / store.get_backup(target)["manifest_path"]
        backup_path = backup_path.parent / manifest["file_name"]
        print(f"== 校验备份 {target} ==")
        # sha
        actual = sha256_file(backup_path)
        expected = manifest.get("sha256")
        if expected:
            match = actual == expected
            print(f"  SHA256: {'OK' if match else 'FAIL'}  "
                  f"(expected={expected[:12]}... actual={actual[:12]}...)")
            ok = ok and match
        # sqlite integrity + 结构
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td) / "v.db"
            if manifest.get("compressed"):
                gzip_decompress(backup_path, tmp)
            else:
                shutil.copy2(backup_path, tmp)
            if not safe_exists_db(tmp):
                print(f"  备份文件无法作为 SQLite 打开: FAIL")
                return 4
            with closing(connect_readonly(tmp)) as conn:
                bad = pragma_integrity_check(conn)
                print(f"  integrity_check: {'OK' if not bad else 'FAIL - ' + str(bad)}")
                ok = ok and not bad
                stats = get_db_stats(conn)
            # 与 manifest.stats 对比（如果有原 db 参照）
            ref_path = args.reference
            if ref_path and safe_exists_db(Path(ref_path)):
                with closing(connect_readonly(Path(ref_path))) as rc:
                    rstats = get_db_stats(rc)
                ok = _compare_structure(rstats, stats, Path(ref_path), tmp) and ok
        print("总体:", "PASS" if ok else "FAIL")
        return 0 if ok else 1

    # 2) 校验快照（检查文件完整性 + 依赖链是否可达）
    if target.startswith("sn_"):
        if store._read_index()["snapshots"].get(target) is None:
            print(f"[!] 快照不存在: {target}", file=sys.stderr)
            return 2
        payload = store.load_snapshot(target)
        print(f"== 校验快照 {target} ==")
        base = payload.get("base_backup")
        if base:
            has_base = store.get_backup(base) is not None
            print(f"  基准备份 {base}: {'存在' if has_base else '缺失'}")
            ok = ok and has_base
        chain = _snapshot_chain(store, target)
        print(f"  依赖链长度 (不含 backup): {len(chain)}")
        for s in chain:
            p = store._read_index()["snapshots"].get(s)
            if p is None:
                ok = False
                print(f"    - {s}: MISSING")
            else:
                print(f"    - {s}: ok")
        print("总体:", "PASS" if ok else "FAIL")
        return 0 if ok else 1

    # 3) 校验某个本地 db（integrity + 可选 vs manifest 结构）
    db_path = Path(target)
    if not safe_exists_db(db_path):
        print(f"[!] {target} 不是有效的 SQLite 文件", file=sys.stderr)
        return 2
    print(f"== 校验本地数据库 {db_path} ==")
    with closing(connect_readonly(db_path)) as conn:
        bad = pragma_integrity_check(conn)
        print(f"  integrity_check: {'OK' if not bad else 'FAIL - ' + str(bad)}")
        ok = ok and not bad
        stats = get_db_stats(conn)
    print(f"  page_size={stats['page_size']}  tables={stats['table_count']}  "
          f"total_rows={stats['total_rows']}")
    if args.reference and safe_exists_db(Path(args.reference)):
        with closing(connect_readonly(Path(args.reference))) as rc:
            rstats = get_db_stats(rc)
        ok = _compare_structure(rstats, stats, Path(args.reference), db_path) and ok
    print("总体:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


def _compare_structure(ref: Dict, cur: Dict, ref_path: Path, cur_path: Path) -> bool:
    ok = True
    ref_t = set(ref["tables"])
    cur_t = set(cur["tables"])
    missing = ref_t - cur_t
    extra = cur_t - ref_t
    if missing:
        print(f"  缺失表: {sorted(missing)}")
        ok = False
    if extra:
        print(f"  多余表: {sorted(extra)}")
    # 对比每个公共表的列
    with closing(connect_readonly(ref_path)) as a, \
         closing(connect_readonly(cur_path)) as b:
        for t in sorted(ref_t & cur_t):
            sa = get_table_schema(a, t)
            sb = get_table_schema(b, t)
            an = {c["name"]: (c["type"], c["pk"]) for c in sa["columns"]}
            bn = {c["name"]: (c["type"], c["pk"]) for c in sb["columns"]}
            diff = False
            for c in sorted(set(an) | set(bn)):
                if an.get(c) != bn.get(c):
                    print(f"    {t}.{c}: ref={an.get(c)} cur={bn.get(c)}")
                    diff = True
            if diff:
                ok = False
    if missing or extra:
        ok = False
    return ok


# ------------------------------- history ------------------------------- #

def cmd_history(args: argparse.Namespace) -> int:
    store = Store(Path(args.store))
    idx = store.index()
    backups = idx["backups"]
    snapshots = idx["snapshots"]

    if not backups and not snapshots:
        print("[i] 暂无备份/快照记录")
        return 0

    print("=== 备份 (backups) ===")
    for bid, meta in backups.items():
        try:
            m = store.load_backup_manifest(bid)
            stats = m.get("stats", {})
            size = m.get("file_size", 0)
            comment = m.get("comment", "")
        except Exception:
            m = {}; stats = {}; size = 0; comment = ""
        print(f"  {bid}  {meta['created_at']}  "
              f"name={m.get('original_name','?')}  size={_human_size(size)}  "
              f"tables={stats.get('table_count','?')}  rows={stats.get('total_rows','?')}"
              + (f"  # {comment}" if comment else ""))

    print("\n=== 快照 (snapshots) ===")
    # 按 base_backup 分组，再按 parent 串联
    groups: Dict[str, List[str]] = {}
    for sid, meta in snapshots.items():
        bb = meta.get("base_backup") or "(no_base)"
        groups.setdefault(bb, []).append(sid)

    for bb, sids in groups.items():
        print(f"  基准备份: {bb}")
        # 构建 parent 图
        children: Dict[str, List[str]] = {}
        roots = []
        meta_map = {sid: snapshots[sid] for sid in sids}
        for sid in sids:
            p = meta_map[sid].get("parent")
            if p and p in meta_map:
                children.setdefault(p, []).append(sid)
            else:
                roots.append(sid)
        for root in roots:
            _print_snapshot_tree(store, root, children, prefix="    ")
    return 0


def _print_snapshot_tree(store: Store, sid: str, children: Dict[str, List[str]],
                         prefix: str) -> None:
    try:
        s = store.load_snapshot(sid)
        summary = s.get("summary", {})
        tag = (f"+{summary.get('added_rows',0)} "
               f"-{summary.get('removed_rows',0)} "
               f"~{summary.get('modified_rows',0)}")
    except Exception:
        tag = "?"
    print(f"{prefix}└─ {sid}  {tag}")
    for i, ch in enumerate(children.get(sid, [])):
        _print_snapshot_tree(store, ch, children, prefix + "   ")


# --------------------------------- diff --------------------------------- #

def cmd_diff(args: argparse.Namespace) -> int:
    a, b = Path(args.db1), Path(args.db2)
    for p in (a, b):
        if not safe_exists_db(p):
            print(f"[!] 不是有效 SQLite 文件: {p}", file=sys.stderr)
            return 2
    ignore = set(args.ignore or [])
    payload = _diff_databases(a, b, b.name, ignore_tables=ignore)
    report = _render_diff_text(payload, a, b)
    print(report)
    if args.markdown:
        md = _render_diff_markdown(payload, a, b)
        out_path = Path(args.markdown)
        out_path.write_text(md, encoding="utf-8")
        print(f"\n[i] Markdown 报告已导出: {out_path}")
    return 0


def _render_diff_text(p: Dict[str, Any], a: Path, b: Path) -> str:
    lines = []
    lines.append(f"=== {a}  vs  {b} ===")
    s = p["summary"]
    lines.append(f"结构: +{len(s['added_tables'])}表  "
                 f"-{len(s['removed_tables'])}表  "
                 f"~{len(s['schema_changed_tables'])}表")
    lines.append(f"行:   +{s['added_rows']}  -{s['removed_rows']}  ~{s['modified_rows']}")

    if s["added_tables"]:
        lines.append(f"\n新增表: {', '.join(s['added_tables'])}")
    if s["removed_tables"]:
        lines.append(f"删除表: {', '.join(s['removed_tables'])}")

    for t, patch in p["tables"].items():
        schema = patch.get("schema", {})
        rows = patch.get("rows", {})
        lines.append(f"\n--- {t} ---")
        if schema.get("changed"):
            if schema.get("added"):
                lines.append(f"  新增列: {', '.join(c['name'] for c in schema['added'])}")
            if schema.get("removed"):
                lines.append(f"  删除列: {', '.join(c['name'] for c in schema['removed'])}")
            for m in schema.get("modified", []):
                lines.append(f"  修改列 {m['name']}: {m['changes']}")
        if rows.get("skipped"):
            lines.append(f"  行对比跳过: {rows.get('reason','')}")
            continue
        lines.append(f"  行: +{rows.get('added_count',0)}  "
                     f"-{rows.get('removed_count',0)}  "
                     f"~{rows.get('modified_count',0)}")
        # 展示样例
        for r in rows.get("added", [])[:3]:
            lines.append(f"    [+] key={r['key']} sample_values="
                         f"{_truncate(str(r['values']), 120)}")
        for r in rows.get("removed", [])[:3]:
            lines.append(f"    [-] key={r['key']} sample_values="
                         f"{_truncate(str(r['values']), 120)}")
        for r in rows.get("modified", [])[:3]:
            lines.append(f"    [~] key={r['key']}  fields="
                         f"{_truncate(str(r['fields']), 160)}")
    return "\n".join(lines)


def _render_diff_markdown(p: Dict[str, Any], a: Path, b: Path) -> str:
    s = p["summary"]
    lines = ["# SQLite 数据库差异报告", ""]
    lines.append(f"- 生成时间: {now_iso()}")
    lines.append(f"- 源库: `{a}`")
    lines.append(f"- 目标库: `{b}`")
    lines.append("")
    lines.append("## 总览")
    lines.append("")
    lines.append("| 指标 | 值 |")
    lines.append("| --- | --- |")
    lines.append(f"| 新增表 | {len(s['added_tables'])} |")
    lines.append(f"| 删除表 | {len(s['removed_tables'])} |")
    lines.append(f"| 结构变化表 | {len(s['schema_changed_tables'])} |")
    lines.append(f"| 新增行 | {s['added_rows']} |")
    lines.append(f"| 删除行 | {s['removed_rows']} |")
    lines.append(f"| 修改行 | {s['modified_rows']} |")
    lines.append("")

    lines.append("## 结构变化")
    lines.append("")
    if s["added_tables"]:
        lines.append("**新增表:** " + ", ".join(f"`{t}`" for t in s["added_tables"]))
        lines.append("")
    if s["removed_tables"]:
        lines.append("**删除表:** " + ", ".join(f"`{t}`" for t in s["removed_tables"]))
        lines.append("")

    for t, patch in p["tables"].items():
        schema = patch.get("schema", {})
        if not schema.get("changed"):
            continue
        lines.append(f"### 表 `{t}` 结构变化")
        lines.append("")
        if schema.get("added"):
            lines.append("**新增列:**")
            lines.append("")
            lines.append("| 列名 | 类型 | NOT NULL | DEFAULT | PK |")
            lines.append("| --- | --- | --- | --- | --- |")
            for c in schema["added"]:
                lines.append(f"| `{c['name']}` | {c['type']} | {bool(c.get('notnull'))} | "
                             f"{c.get('default')} | {c.get('pk',0)} |")
            lines.append("")
        if schema.get("removed"):
            lines.append("**删除列:**")
            lines.append("")
            lines.append("| 列名 | 类型 | NOT NULL | DEFAULT | PK |")
            lines.append("| --- | --- | --- | --- | --- |")
            for c in schema["removed"]:
                lines.append(f"| `{c['name']}` | {c['type']} | {bool(c.get('notnull'))} | "
                             f"{c.get('default')} | {c.get('pk',0)} |")
            lines.append("")
        if schema.get("modified"):
            lines.append("**修改列:**")
            lines.append("")
            lines.append("| 列名 | 变化 |")
            lines.append("| --- | --- |")
            for m in schema["modified"]:
                lines.append(f"| `{m['name']}` | ```{json.dumps(m['changes'], ensure_ascii=False)}``` |")
            lines.append("")

    lines.append("## 行级变化")
    lines.append("")
    for t, patch in p["tables"].items():
        rows = patch.get("rows", {})
        if rows.get("skipped"):
            continue
        added_n = rows.get("added_count", 0)
        removed_n = rows.get("removed_count", 0)
        modified_n = rows.get("modified_count", 0)
        if added_n == removed_n == modified_n == 0:
            continue
        lines.append(f"### 表 `{t}` 行变化 (+{added_n}/-{removed_n}/~{modified_n})")
        lines.append("")
        if rows.get("added"):
            lines.append("**新增样例:**")
            lines.append("")
            lines.append("```json")
            for r in rows["added"][:10]:
                lines.append(json.dumps(r, ensure_ascii=False, default=_jsonable))
            lines.append("```")
            lines.append("")
        if rows.get("removed"):
            lines.append("**删除样例:**")
            lines.append("")
            lines.append("```json")
            for r in rows["removed"][:10]:
                lines.append(json.dumps(r, ensure_ascii=False, default=_jsonable))
            lines.append("```")
            lines.append("")
        if rows.get("modified"):
            lines.append("**修改样例:**")
            lines.append("")
            lines.append("```json")
            for r in rows["modified"][:10]:
                lines.append(json.dumps(r, ensure_ascii=False, default=_jsonable))
            lines.append("```")
            lines.append("")

    return "\n".join(lines)


def _truncate(s: str, n: int) -> str:
    return s if len(s) <= n else s[: n - 1] + "…"


# -------------------------------- prune -------------------------------- #

def cmd_prune(args: argparse.Namespace) -> int:
    store = Store(Path(args.store))
    idx = store.index()
    backups = idx["backups"]
    snapshots = idx["snapshots"]

    keep_last = args.keep_last
    keep_daily = args.keep_daily
    keep_weekly = args.keep_weekly

    if not backups:
        print("[i] 没有备份，无需清理")
        return 0

    # 按 original_name 分组
    by_db: Dict[str, List[Tuple[str, Dict]]] = {}
    for bid, meta in backups.items():
        try:
            m = store.load_backup_manifest(bid)
            name = m.get("original_name") or "(unknown)"
        except Exception:
            name = "(unknown)"
        by_db.setdefault(name, []).append((bid, meta))

    to_delete_bids: set = set()
    reason_lines = []

    for name, items in by_db.items():
        # 按创建时间排序（新->旧）
        items_sorted = sorted(items, key=lambda kv: kv[1]["created_at"], reverse=True)

        # 1) 最近 N 个
        keep_ids = set()
        for bid, _ in items_sorted[:keep_last]:
            keep_ids.add(bid)

        # 2) 按天桶：每天保留最新一个
        if keep_daily > 0:
            day_bucket: Dict[str, Tuple[str, Dict]] = {}
            for bid, meta in items_sorted:
                day = meta["created_at"][:10]
                if day not in day_bucket:
                    day_bucket[day] = (bid, meta)
            days = sorted(day_bucket.keys(), reverse=True)
            for d in days[:keep_daily]:
                keep_ids.add(day_bucket[d][0])

        # 3) 按周桶：每周保留最新一个
        if keep_weekly > 0:
            week_bucket: Dict[str, Tuple[str, Dict]] = {}
            for bid, meta in items_sorted:
                try:
                    d = dt.datetime.fromisoformat(meta["created_at"])
                    iso = d.isocalendar()
                    week_key = f"{iso[0]}-W{iso[1]:02d}"
                except Exception:
                    week_key = meta["created_at"][:7]
                if week_key not in week_bucket:
                    week_bucket[week_key] = (bid, meta)
            weeks = sorted(week_bucket.keys(), reverse=True)
            for w in weeks[:keep_weekly]:
                keep_ids.add(week_bucket[w][0])

        for bid, meta in items_sorted:
            if bid not in keep_ids:
                to_delete_bids.add(bid)
                reason_lines.append(f"  删除备份 {bid} ({name} {meta['created_at']})")

    # 快照依赖分析：只保留在保留备份之上、且仍有链根的快照
    remaining_bids = set(backups.keys()) - to_delete_bids
    # 快照如果它的 base_backup 要被删，且没有其他可锚定的点，则一同删除
    to_delete_sids: set = set()
    for sid, meta in snapshots.items():
        bb = meta.get("base_backup")
        if bb and bb in to_delete_bids:
            # 链首在删除的备份上，一并删
            to_delete_sids.add(sid)
            reason_lines.append(f"  删除快照 {sid} (因 base_backup={bb} 将被删除)")
        elif bb and bb in remaining_bids:
            pass
        elif not bb:
            # 没 base 的快照，保持不动（保守策略）
            pass

    # 再扫描：若快照 parent 在被删集合里，也删（链中间断了）
    changed = True
    while changed:
        changed = False
        for sid, meta in snapshots.items():
            if sid in to_delete_sids:
                continue
            p = meta.get("parent")
            if p and p.startswith("sn_") and p in to_delete_sids:
                to_delete_sids.add(sid)
                reason_lines.append(f"  删除快照 {sid} (因 parent={p} 将被删除)")
                changed = True

    print("=== 清理计划 ===")
    print(f"  保留规则: --keep-last={keep_last} --keep-daily={keep_daily} --keep-weekly={keep_weekly}")
    print(f"  将删除备份: {len(to_delete_bids)} 个")
    print(f"  将删除快照: {len(to_delete_sids)} 个")
    if reason_lines:
        for l in reason_lines:
            print(l)
    else:
        print("  （无内容需要删除）")

    if not to_delete_bids and not to_delete_sids:
        return 0

    if not args.yes:
        print("\n使用 --yes 真正执行删除，或复查上述计划。")
        return 0

    # 执行删除
    for bid in to_delete_bids:
        bdir = store.backups_dir / bid
        if bdir.exists():
            shutil.rmtree(bdir, ignore_errors=True)
        del idx["backups"][bid]
    for sid in to_delete_sids:
        sdir = store.snapshots_dir / sid
        if sdir.exists():
            shutil.rmtree(sdir, ignore_errors=True)
        del idx["snapshots"][sid]
    store._write_index(idx)
    print(f"\n[✓] 已删除备份 {len(to_delete_bids)} 个, 快照 {len(to_delete_sids)} 个")
    return 0


# ------------------------------- CLI 入口 ------------------------------- #

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="sqlite_backup.py",
        description="SQLite 备份/恢复/增量快照/差异/清理 综合工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--store", default=str(BACKUP_DIR),
                   help=f"备份仓库目录（默认: {BACKUP_DIR}）")

    sub = p.add_subparsers(dest="cmd", required=True)

    # init-demo
    s = sub.add_parser("init-demo", help="生成示例数据库 (users/orders/logs + 演示数据)")
    s.add_argument("--db", default="app.db", help="输出数据库路径")
    s.add_argument("--force", action="store_true", help="存在则覆盖")
    s.set_defaults(func=cmd_init_demo)

    # backup
    s = sub.add_parser("backup", help="全量备份")
    s.add_argument("db", help="源 SQLite 文件")
    s.add_argument("--gzip", action="store_true", help="使用 gzip 压缩备份文件")
    s.add_argument("--comment", default=None, help="备注（将写入 manifest）")
    s.set_defaults(func=cmd_backup)

    # snapshot
    s = sub.add_parser("snapshot", help="生成增量快照")
    s.add_argument("db", help="当前 SQLite 文件")
    s.add_argument("--parent", default=None,
                   help="基线：bk_... 备份 ID 或 sn_... 快照 ID；缺省自动找同名 db 的最新")
    s.add_argument("--ignore", action="append", default=None,
                   help="忽略的表名，可重复使用")
    s.set_defaults(func=cmd_snapshot)

    # restore
    s = sub.add_parser("restore", help="从全量备份恢复")
    s.add_argument("backup", help="备份 ID (bk_...) 或备份文件路径 (.db/.gz)")
    s.add_argument("-o", "--output", default=None, help="输出路径，默认用原文件名")
    s.add_argument("--dry-run", action="store_true", help="仅显示将要做的动作，不写盘")
    s.add_argument("--force", action="store_true", help="校验失败/无法建安全副本时仍继续")
    s.set_defaults(func=cmd_restore)

    # apply
    s = sub.add_parser("apply", help="将增量快照应用到目标数据库")
    s.add_argument("snapshot", help="快照 ID (sn_...) 或 snapshot.json 路径")
    s.add_argument("target", help="要应用的目标数据库")
    s.add_argument("--dry-run", action="store_true", help="仅显示计划")
    s.set_defaults(func=cmd_apply)

    # verify
    s = sub.add_parser("verify", help="校验备份/快照/本地数据库")
    s.add_argument("target", help="bk_... / sn_... / 本地 .db 路径")
    s.add_argument("--reference", default=None, help="结构参照的数据库路径（可选）")
    s.set_defaults(func=cmd_verify)

    # history
    s = sub.add_parser("history", help="列出备份链路与快照依赖关系")
    s.set_defaults(func=cmd_history)

    # diff
    s = sub.add_parser("diff", help="对比两个数据库，输出结构与数据差异")
    s.add_argument("db1", help="源数据库")
    s.add_argument("db2", help="目标数据库")
    s.add_argument("--ignore", action="append", default=None, help="忽略的表名，可重复")
    s.add_argument("--markdown", default=None, help="导出 Markdown 报告到此文件")
    s.set_defaults(func=cmd_diff)

    # prune
    s = sub.add_parser("prune", help="按保留策略清理旧备份/快照")
    s.add_argument("--keep-last", type=int, default=3, help="每个 db 保留最近 N 个（默认 3）")
    s.add_argument("--keep-daily", type=int, default=7, help="每个 db 每天保留 N 天（默认 7）")
    s.add_argument("--keep-weekly", type=int, default=4, help="每个 db 每周保留 N 周（默认 4）")
    s.add_argument("--yes", action="store_true", help="无需交互确认直接执行删除")
    s.set_defaults(func=cmd_prune)

    return p


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args) or 0
    except KeyboardInterrupt:
        print("\n[!] 已中断", file=sys.stderr)
        return 130
    except (sqlite3.Error, OSError, RuntimeError, KeyError) as e:
        print(f"[!] 运行错误: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())

