from dataclasses import dataclass, field, asdict
from typing import Dict, List, Any, Optional
from common.util import hostname


@dataclass
class SQLAnywhereConn:
    astart: str
    dbf: str
    dbn: str
    driver: str
    host: str
    pwd: str
    server: str
    uid: str
    port: Optional[int] = None

    def to_dict(self):
        return asdict(self)


# BATCHER #####################################################################


@dataclass
class BatcherTaskBody:
    asset: str
    chunk: int
    cron: str
    id: int
    recency: int
    repo_fs_path: str
    repo_id: str
    repo_name: str
    suite: str
    tag: str
    where_clause: str

    def to_dict(self):
        return asdict(self)


@dataclass
class BatcherTask:
    body: BatcherTaskBody
    directive: str
    id: int
    status: str
    worker: str

    def to_dict(self):
        task_dict = asdict(self)
        task_dict["body"] = self.body.to_dict()
        return task_dict


# LOADER ######################################################################


@dataclass
class LoaderTaskBody:
    asset: str
    asset_id_keys: List[str]
    batch_id: str
    conn: SQLAnywhereConn
    prefixes: Dict[str, str]
    repo_id: str
    repo_name: str
    selector: str
    suite: str
    tag: str
    well_id_keys: List[str]
    xforms: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self):
        body_dict = asdict(self)
        body_dict["conn"] = self.conn.to_dict()
        return body_dict


@dataclass
class LoaderTask:
    body: LoaderTaskBody
    directive: str
    id: int
    status: str
    worker: str

    def to_dict(self):
        task_dict = asdict(self)
        task_dict["body"] = self.body.to_dict()
        return task_dict


# RECON #######################################################################


@dataclass
class ReconTaskBody:
    ggx_host: str
    recon_root: str
    suite: str

    def to_dict(self):
        return asdict(self)


@dataclass
class ReconTask:
    body: ReconTaskBody
    directive: str
    id: int
    status: str
    worker: str

    def to_dict(self):
        task_dict = asdict(self)
        task_dict["body"] = self.body.to_dict()
        return task_dict


# REPO ########################################################################
@dataclass
class ConnAux:
    ggx_host: str

    def to_dict(self):
        return asdict(self)


@dataclass
class Repo:
    id: str
    name: str
    fs_path: str
    conn: SQLAnywhereConn
    conn_aux: ConnAux
    suite: str
    well_count: int
    wells_with_completion: int
    wells_with_core: int
    wells_with_dst: int
    wells_with_formation: int
    wells_with_ip: int
    wells_with_perforation: int
    wells_with_production: int
    wells_with_raster_log: int
    wells_with_survey: int
    wells_with_vector_log: int
    wells_with_zone: int
    storage_epsg: int
    storage_name: str
    display_epsg: int
    display_name: str
    files: int
    directories: int
    bytes: int
    repo_mod: str
    outline: List[List[float]] = field(default_factory=list)
    active: Optional[bool] = True

    def to_dict(self):
        repo_dict = asdict(self)
        repo_dict["conn"] = self.conn.to_dict()
        repo_dict["conn_aux"] = self.conn_aux.to_dict()
        return repo_dict


# SEARCH ######################################################################


@dataclass
class SearchTaskBody:
    tag: str

    def to_dict(self):
        return asdict(self)


@dataclass
class SearchTask:
    body: SearchTaskBody
    directive: str
    id: int
    status: str
    worker: str

    def to_dict(self):
        task_dict = asdict(self)
        task_dict["body"] = self.body.to_dict()
        return task_dict


# #############################################################################


def validate_repo(payload: dict):
    # remove supabase audit columns
    unwanted_keys = ["active", "created_at", "touched_at", "updated_at"]
    for key in unwanted_keys:
        if key in payload:
            payload.pop(key)

    return Repo(
        id=payload["id"],
        name=payload["name"],
        fs_path=payload["fs_path"],
        conn=SQLAnywhereConn(**payload["conn"]),
        conn_aux=ConnAux(**payload["conn_aux"]),
        suite=payload["suite"],
        well_count=payload["well_count"],
        wells_with_completion=payload["wells_with_completion"],
        wells_with_core=payload["wells_with_core"],
        wells_with_dst=payload["wells_with_dst"],
        wells_with_formation=payload["wells_with_formation"],
        wells_with_ip=payload["wells_with_ip"],
        wells_with_perforation=payload["wells_with_perforation"],
        wells_with_production=payload["wells_with_production"],
        wells_with_raster_log=payload["wells_with_raster_log"],
        wells_with_survey=payload["wells_with_survey"],
        wells_with_vector_log=payload["wells_with_vector_log"],
        wells_with_zone=payload["wells_with_zone"],
        storage_epsg=payload["storage_epsg"],
        storage_name=payload["storage_name"],
        display_epsg=payload["display_epsg"],
        display_name=payload["display_name"],
        files=payload["files"],
        directories=payload["directories"],
        bytes=payload["bytes"],
        repo_mod=payload["repo_mod"],
        outline=payload["outline"],
    )


def validate_task(payload: dict):

    try:
        if payload["record"]:
            if (
                payload["record"]["worker"] == hostname()
                and payload["record"]["status"] == "PENDING"
                and (
                    (
                        "suite" in payload["record"]["body"]
                        and payload["record"]["body"]["suite"] == "geographix"
                    )
                    or (
                        "suites" in payload["record"]["body"]
                        and "geographix" in payload["record"]["body"]["suites"]
                    )
                )
            ):
                task = payload["record"]

                if task.get("directive") == "batcher":
                    return BatcherTask(
                        body=BatcherTaskBody(**task["body"]),
                        directive=task["directive"],
                        id=task["id"],
                        status=task["status"],
                        worker=task["worker"],
                    )

                if task.get("directive") == "loader":
                    return LoaderTask(
                        body=LoaderTaskBody(**task["body"]),
                        directive=task["directive"],
                        id=task["id"],
                        status=task["status"],
                        worker=task["worker"],
                    )

                if task.get("directive") == "recon":
                    return ReconTask(
                        body=ReconTaskBody(**task["body"]),
                        directive=task["directive"],
                        id=task["id"],
                        status=task["status"],
                        worker=task["worker"],
                    )

                if task.get("directive") == "search":
                    print("...................................")
                    print("search")
                    # print(payload.get("record").get("body").keys())
                    # dc = make_data_class(task)
                    # print(dc)
                    # return dc
                    print("...................................")
                    pass

    except KeyError as ke:
        print(ke)
        return None
