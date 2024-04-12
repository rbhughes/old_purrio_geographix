from common.sqlanywhere import db_exec
from concave_hull import concave_hull

NOTNULL_LONLAT = (
    "SELECT surface_longitude, surface_latitude FROM well "
    "WHERE surface_longitude IS NOT NULL and surface_latitude IS NOT NULL"
)

HULL_CONCAVITY = 2

##########

WELLS = "SELECT COUNT(uwi) AS tally FROM well"
WELLS_WITH_COMPLETION = "SELECT COUNT(DISTINCT uwi) AS tally FROM well_completion"

# const WELLS_WITH_COMPLETION = `
# SELECT COUNT(DISTINCT uwi) AS tally FROM (
#     SELECT uwi FROM well_completion
#     UNION
#     SELECT uwi FROM well_treatment
# ) x`;

WELLS_WITH_CORE = "SELECT COUNT(DISTINCT uwi) AS tally FROM well_core"

WELLS_WITH_DST = (
    "SELECT COUNT(DISTINCT uwi) AS tally FROM well_test WHERE test_type = 'DST'"
)

WELLS_WITH_FORMATION = "SELECT COUNT(DISTINCT uwi) AS tally FROM well_formation"

# to match the selector for IP:
# SELECT count(DISTINCT uwi || source || run_number)
#   from well_test WHERE test_type ='IP'

WELLS_WITH_IP = (
    "SELECT COUNT(DISTINCT uwi) AS tally FROM well_test WHERE test_type = 'IP'"
)

WELLS_WITH_PERFORATION = "SELECT COUNT(DISTINCT uwi) AS tally FROM well_perforation"

WELLS_WITH_PRODUCTION = (
    "SELECT COUNT(DISTINCT uwi) AS tally FROM well_cumulative_production"
)

WELLS_WITH_RASTER_LOG = (
    "SELECT COUNT(DISTINCT(w.uwi)) AS tally FROM well w "
    "JOIN log_image_reg_log_section r ON r.well_id = w.uwi"
)

WELLS_WITH_SURVEY = (
    "SELECT COUNT(DISTINCT uwi) AS tally FROM ( "
    "SELECT uwi FROM well_dir_srvy_station "
    "UNION "
    "SELECT uwi FROM well_dir_proposed_srvy_station "
    ") x"
)

WELLS_WITH_VECTOR_LOG = "SELECT COUNT(DISTINCT wellid) AS tally FROM gx_well_curve"

WELLS_WITH_ZONE = "SELECT COUNT(DISTINCT uwi) AS tally FROM well_zone_interval"


def well_counts(repo):
    """doc"""
    counter_sql = {
        "well_count": WELLS,
        "wells_with_completion": WELLS_WITH_COMPLETION,
        "wells_with_core": WELLS_WITH_CORE,
        "wells_with_dst": WELLS_WITH_DST,
        "wells_with_formation": WELLS_WITH_FORMATION,
        "wells_with_ip": WELLS_WITH_IP,
        "wells_with_perforation": WELLS_WITH_PERFORATION,
        "wells_with_production": WELLS_WITH_PRODUCTION,
        "wells_with_raster_log": WELLS_WITH_RASTER_LOG,
        "wells_with_survey": WELLS_WITH_SURVEY,
        "wells_with_vector_log": WELLS_WITH_VECTOR_LOG,
        "wells_with_zone": WELLS_WITH_ZONE,
    }

    res = db_exec(repo.get("conn"), list(counter_sql.values()))

    counts = {}

    for i, k in enumerate(counter_sql.keys()):
        counts[k] = res[i][0]["tally"] or 0

    return counts


def hull_outline(repo):
    res = db_exec(repo.get("conn"), NOTNULL_LONLAT)
    points = [[r["surface_longitude"], r["surface_latitude"]] for r in res]

    if len(points) < 3:
        print(f"Too few valid Lon/Lat points for polygon: {repo.get('name')}")
        return {"outline": None}

    hull = concave_hull(points, concavity=HULL_CONCAVITY)
    first_point = hull[0]
    hull.append(first_point)
    return {"outline": hull}
