# recon_task = {
#     "body": {
#         "ggx_host": "SCARAB",
#         "kingdom_password": "",
#         "kingdom_server": "",
#         "kingdom_username": "",
#         "recon_root": "//scarab/ggx_projects/stratton",
#         "suite": "geographix",
#         "worker": "scarab",
#     },
#     "directive": "recon",
#     "id": 94845,
#     "status": "PENDING",
#     "worker": "scarab",
# }
#
# batcher_task = {
#     "body": {
#         "asset": "well",
#         "chunk": 3,
#         "cron": "",
#         "id": 76,
#         "recency": 0,
#         "repo_fs_path": "//scarab/ggx_projects/stratton",
#         "repo_id": "e68fa3e5-9e8b-18e0-e690-9839d0dc0f22",
#         "repo_name": "stratton",
#         "suite": "geographix",
#         "tag": "GRINKLE",
#         "where_clause": "",
#     },
#     "directive": "batcher",
#     "id": 94846,
#     "status": "PENDING",
#     "worker": "scarab",
# }
#
# loader_task = {
#     'body': {
#         'asset': 'well',
#         'asset_id_keys': ['w_uwi'],
#         'batch_id': '85bc44dd0efe5b90d78e8b8938b3cb72',
#         'conn': {
#             'astart': 'YES',
#             'dbf': '//scarab/ggx_projects/stratton/gxdb.db',
#             'dbn': 'stratton-ggx_projects',
#             'driver': 'SQL Anywhere 17',
#             'host': 'SCARAB',
#             'pwd': 'sql',
#             'server': 'GGX_SCARAB',
#             'uid': 'dba'},
#         'prefixes': {'c_': 'legal_congress_loc', 'w_': 'well'},
#         'repo_id': 'e68fa3e5-9e8b-18e0-e690-9839d0dc0f22',
#         'repo_name': 'stratton',
#         'selector': 'mystring',
#         'suite': 'geographix',
#         'tag': 'GRINKLE',
#         'well_id_keys': ['w_uwi'],
#         'xforms': {}
#     },
#     "directive": "loader",
#     "id": 94846,
#     "status": "PENDING",
#     "worker": "scarab",
# }

from dataclasses import dataclass, field
from typing import Dict, List, Union, Any, Optional


@dataclass
class SQLAnywhereConnection:
    astart: str
    dbf: str
    dbn: str
    driver: str
    host: str
    pwd: str
    server: str
    uid: str
    port: Optional[int]


@dataclass
class LoaderBody:
    asset: str
    asset_id_keys: List[str]
    batch_id: str
    conn: SQLAnywhereConnection
    prefixes: Dict[str, str]
    repo_id: str
    repo_name: str
    selector: str
    suite: str
    tag: str
    well_id_keys: List[str]
    xforms: Dict[str, Any] = field(default_factory=dict)


@dataclass
class LoaderTask:
    body: LoaderBody
    directive: str
    id: int
    status: str
    worker: str


def make_data_class(task: Dict):
    dc = LoaderTask(
        body=LoaderBody(**task["body"]),
        directive=task["directive"],
        id=task["id"],
        status=task["status"],
        worker=task["worker"],
    )
    return dc
