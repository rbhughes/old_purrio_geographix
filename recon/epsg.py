import os
import re
import xml.etree.ElementTree as ET


geodetics = [
    {"geog": "gcs_north_american_1927", "datum": "d_north_american_1927", "code": 4267},
    {"geog": "gcs_north_american_1983", "datum": "d_north_american_1983", "code": 4269},
    {"geog": "gcs_wgs_1984", "datum": "d_wgs_1984", "code": 4236},
    {"geog": "gcs_wgs_1972", "datum": "d_wgs_1972", "code": 6322},
    {"geog": "geographic_latitude_longitude", "datum": "cape_canaveral", "code": 6717},
    {"geog": "geographic_latitude_longitude", "datum": "itrf_1997_0", "code": 6655},
    {"geog": "geographic_latitude_longitude", "datum": "itrf_1994", "code": 6653},
    {"geog": "gcs_clarke_1866", "datum": "d_clarke_1866", "code": 4008},
]

projections = [
    {
        "proj": "spcs27__alabama_east",
        "geog": "gcs_north_american_1927",
        "code": 26729,
    },
    {
        "proj": "spcs27__alabama_west",
        "geog": "gcs_north_american_1927",
        "code": 26730,
    },
    {
        "proj": "spcs27__arizona_central",
        "geog": "gcs_north_american_1927",
        "code": 26749,
    },
    {
        "proj": "spcs27__arizona_east",
        "geog": "gcs_north_american_1927",
        "code": 26748,
    },
    {
        "proj": "spcs27__arizona_west",
        "geog": "gcs_north_american_1927",
        "code": 26750,
    },
    {
        "proj": "spcs27__arkansas_north",
        "geog": "gcs_north_american_1927",
        "code": 26751,
    },
    {
        "proj": "spcs27__arkansas_south",
        "geog": "gcs_north_american_1927",
        "code": 26752,
    },
    {
        "proj": "spcs27__california_i",
        "geog": "gcs_north_american_1927",
        "code": 26741,
    },
    {
        "proj": "spcs27__california_ii",
        "geog": "gcs_north_american_1927",
        "code": 26742,
    },
    {
        "proj": "spcs27__california_iii",
        "geog": "gcs_north_american_1927",
        "code": 26743,
    },
    {
        "proj": "spcs27__california_iv",
        "geog": "gcs_north_american_1927",
        "code": 26744,
    },
    {
        "proj": "spcs27__california_v",
        "geog": "gcs_north_american_1927",
        "code": 26745,
    },
    {
        "proj": "spcs27__california_vi",
        "geog": "gcs_north_american_1927",
        "code": 26746,
    },
    {
        "proj": "spcs27__california_vii",
        "geog": "gcs_north_american_1927",
        "code": 26799,
    },
    {
        "proj": "spcs27__colorado_central",
        "geog": "gcs_north_american_1927",
        "code": 26754,
    },
    {
        "proj": "spcs27__colorado_north",
        "geog": "gcs_north_american_1927",
        "code": 26753,
    },
    {
        "proj": "spcs27__colorado_south",
        "geog": "gcs_north_american_1927",
        "code": 26755,
    },
    {
        "proj": "spcs27__connecticut",
        "geog": "gcs_north_american_1927",
        "code": 26756,
    },
    {
        "proj": "spcs27__delaware",
        "geog": "gcs_north_american_1927",
        "code": 26757,
    },
    {
        "proj": "spcs27__florida_east",
        "geog": "gcs_north_american_1927",
        "code": 26758,
    },
    {
        "proj": "spcs27__florida_north",
        "geog": "gcs_north_american_1927",
        "code": 26760,
    },
    {
        "proj": "spcs27__florida_west",
        "geog": "gcs_north_american_1927",
        "code": 26759,
    },
    {
        "proj": "spcs27__georgia_east",
        "geog": "gcs_north_american_1927",
        "code": 26766,
    },
    {
        "proj": "spcs27__georgia_west",
        "geog": "gcs_north_american_1927",
        "code": 26767,
    },
    {
        "proj": "spcs27__idaho_central",
        "geog": "gcs_north_american_1927",
        "code": 26769,
    },
    {
        "proj": "spcs27__idaho_east",
        "geog": "gcs_north_american_1927",
        "code": 26768,
    },
    {
        "proj": "spcs27__idaho_west",
        "geog": "gcs_north_american_1927",
        "code": 26770,
    },
    {
        "proj": "spcs27__illinois_east",
        "geog": "gcs_north_american_1927",
        "code": 26771,
    },
    {
        "proj": "spcs27__illinois_west",
        "geog": "gcs_north_american_1927",
        "code": 26772,
    },
    {
        "proj": "spcs27__indiana_east",
        "geog": "gcs_north_american_1927",
        "code": 26773,
    },
    {
        "proj": "spcs27__indiana_west",
        "geog": "gcs_north_american_1927",
        "code": 26774,
    },
    {
        "proj": "spcs27__iowa_north",
        "geog": "gcs_north_american_1927",
        "code": 26775,
    },
    {
        "proj": "spcs27__iowa_south",
        "geog": "gcs_north_american_1927",
        "code": 26776,
    },
    {
        "proj": "spcs27__kansas_north",
        "geog": "gcs_north_american_1927",
        "code": 26777,
    },
    {
        "proj": "spcs27__kansas_south",
        "geog": "gcs_north_american_1927",
        "code": 26778,
    },
    {
        "proj": "spcs27__kentucky_north",
        "geog": "gcs_north_american_1927",
        "code": 26779,
    },
    {
        "proj": "spcs27__kentucky_south",
        "geog": "gcs_north_american_1927",
        "code": 26780,
    },
    {
        "proj": "spcs27__louisiana_north",
        "geog": "gcs_north_american_1927",
        "code": 26781,
    },
    {
        "proj": "spcs27__louisiana_offshore",
        "geog": "gcs_north_american_1927",
        "code": 32099,
    },
    {
        "proj": "spcs27__louisiana_south",
        "geog": "gcs_north_american_1927",
        "code": 26782,
    },
    {
        "proj": "spcs27__maine_east",
        "geog": "gcs_north_american_1927",
        "code": 26783,
    },
    {
        "proj": "spcs27__maine_west",
        "geog": "gcs_north_american_1927",
        "code": 26784,
    },
    {
        "proj": "spcs27__maryland",
        "geog": "gcs_north_american_1927",
        "code": 26785,
    },
    {
        "proj": "spcs27__massachusetts_island",
        "geog": "gcs_north_american_1927",
        "code": 26787,
    },
    {
        "proj": "spcs27__massachusetts_mainland",
        "geog": "gcs_north_american_1927",
        "code": 26786,
    },
    {
        "proj": "spcs27__michigan_central_lambert",
        "geog": "gcs_north_american_1927",
        "code": 6201,
    },
    {
        "proj": "spcs27__michigan_central_tm_obsolete",
        "geog": "gcs_north_american_1927",
        "code": 5624,
    },
    {
        "proj": "spcs27__michigan_east_tm_obsolete",
        "geog": "gcs_north_american_1927",
        "code": 5623,
    },
    {
        "proj": "spcs27__michigan_north_lambert",
        "geog": "gcs_north_american_1927",
        "code": 6966,
    },
    {
        "proj": "spcs27__michigan_south_lambert",
        "geog": "gcs_north_american_1927",
        "code": 6202,
    },
    {
        "proj": "spcs27__michigan_west_tm_obsolete",
        "geog": "gcs_north_american_1927",
        "code": 5625,
    },
    {
        "proj": "spcs27__minnesota_central",
        "geog": "gcs_north_american_1927",
        "code": 26792,
    },
    {
        "proj": "spcs27__minnesota_north",
        "geog": "gcs_north_american_1927",
        "code": 26791,
    },
    {
        "proj": "spcs27__minnesota_south",
        "geog": "gcs_north_american_1927",
        "code": 26793,
    },
    {
        "proj": "spcs27__mississippi_east",
        "geog": "gcs_north_american_1927",
        "code": 26794,
    },
    {
        "proj": "spcs27__mississippi_west",
        "geog": "gcs_north_american_1927",
        "code": 26795,
    },
    {
        "proj": "spcs27__missouri_central",
        "geog": "gcs_north_american_1927",
        "code": 26797,
    },
    {
        "proj": "spcs27__missouri_east",
        "geog": "gcs_north_american_1927",
        "code": 26796,
    },
    {
        "proj": "spcs27__missouri_west",
        "geog": "gcs_north_american_1927",
        "code": 26798,
    },
    {
        "proj": "spcs27__montana_central",
        "geog": "gcs_north_american_1927",
        "code": 32002,
    },
    {
        "proj": "spcs27__montana_north",
        "geog": "gcs_north_american_1927",
        "code": 32001,
    },
    {
        "proj": "spcs27__montana_south",
        "geog": "gcs_north_american_1927",
        "code": 32003,
    },
    {
        "proj": "spcs27__nebraska_north",
        "geog": "gcs_north_american_1927",
        "code": 32005,
    },
    {
        "proj": "spcs27__nebraska_south",
        "geog": "gcs_north_american_1927",
        "code": 32006,
    },
    {
        "proj": "spcs27__nevada_central",
        "geog": "gcs_north_american_1927",
        "code": 32008,
    },
    {
        "proj": "spcs27__nevada_east",
        "geog": "gcs_north_american_1927",
        "code": 32007,
    },
    {
        "proj": "spcs27__nevada_west",
        "geog": "gcs_north_american_1927",
        "code": 32009,
    },
    {
        "proj": "spcs27__new_hampshire",
        "geog": "gcs_north_american_1927",
        "code": 32010,
    },
    {
        "proj": "spcs27__new_jersey",
        "geog": "gcs_north_american_1927",
        "code": 32011,
    },
    {
        "proj": "spcs27__new_mexico_central",
        "geog": "gcs_north_american_1927",
        "code": 32013,
    },
    {
        "proj": "spcs27__new_mexico_east",
        "geog": "gcs_north_american_1927",
        "code": 32012,
    },
    {
        "proj": "spcs27__new_mexico_west",
        "geog": "gcs_north_american_1927",
        "code": 32014,
    },
    {
        "proj": "spcs27__new_york_central",
        "geog": "gcs_north_american_1927",
        "code": 32016,
    },
    {
        "proj": "spcs27__new_york_east",
        "geog": "gcs_north_american_1927",
        "code": 32015,
    },
    {
        "proj": "spcs27__new_york_long_island",
        "geog": "gcs_north_american_1927",
        "code": 4456,
    },
    {
        "proj": "spcs27__new_york_west",
        "geog": "gcs_north_american_1927",
        "code": 32017,
    },
    {
        "proj": "spcs27__north_carolina",
        "geog": "gcs_north_american_1927",
        "code": 32019,
    },
    {
        "proj": "spcs27__north_dakota_north",
        "geog": "gcs_north_american_1927",
        "code": 32020,
    },
    {
        "proj": "spcs27__north_dakota_south",
        "geog": "gcs_north_american_1927",
        "code": 32021,
    },
    {
        "proj": "spcs27__ohio_north",
        "geog": "gcs_north_american_1927",
        "code": 32022,
    },
    {
        "proj": "spcs27__ohio_south",
        "geog": "gcs_north_american_1927",
        "code": 32023,
    },
    {
        "proj": "spcs27__oklahoma_north",
        "geog": "gcs_north_american_1927",
        "code": 32024,
    },
    {
        "proj": "spcs27__oklahoma_south",
        "geog": "gcs_north_american_1927",
        "code": 32025,
    },
    {
        "proj": "spcs27__oregon_north",
        "geog": "gcs_north_american_1927",
        "code": 32026,
    },
    {
        "proj": "spcs27__oregon_south",
        "geog": "gcs_north_american_1927",
        "code": 32027,
    },
    {
        "proj": "spcs27__pennsylvania_north",
        "geog": "gcs_north_american_1927",
        "code": 32028,
    },
    {
        "proj": "spcs27__pennsylvania_south",
        "geog": "gcs_north_american_1927",
        "code": 4455,
    },
    {
        "proj": "spcs27__rhode_island_south",
        "geog": "gcs_north_american_1927",
        "code": 32030,
    },
    {
        "proj": "spcs27__south_carolina_north",
        "geog": "gcs_north_american_1927",
        "code": 32031,
    },
    {
        "proj": "spcs27__south_carolina_south",
        "geog": "gcs_north_american_1927",
        "code": 32033,
    },
    {
        "proj": "spcs27__south_dakota_north",
        "geog": "gcs_north_american_1927",
        "code": 32034,
    },
    {
        "proj": "spcs27__south_dakota_south",
        "geog": "gcs_north_american_1927",
        "code": 32035,
    },
    {
        "proj": "spcs27__tennessee",
        "geog": "gcs_north_american_1927",
        "code": 2204,
    },
    {
        "proj": "spcs27__texas_central",
        "geog": "gcs_north_american_1927",
        "code": 32039,
    },
    {
        "proj": "spcs27__texas_north",
        "geog": "gcs_north_american_1927",
        "code": 32037,
    },
    {
        "proj": "spcs27__texas_north_central",
        "geog": "gcs_north_american_1927",
        "code": 32038,
    },
    {
        "proj": "spcs27__texas_south",
        "geog": "gcs_north_american_1927",
        "code": 32041,
    },
    {
        "proj": "spcs27__texas_south_central",
        "geog": "gcs_north_american_1927",
        "code": 32040,
    },
    {
        "proj": "spcs27__utah_central",
        "geog": "gcs_north_american_1927",
        "code": 32043,
    },
    {
        "proj": "spcs27__utah_north",
        "geog": "gcs_north_american_1927",
        "code": 32042,
    },
    {
        "proj": "spcs27__utah_south",
        "geog": "gcs_north_american_1927",
        "code": 32044,
    },
    {
        "proj": "spcs27__vermont",
        "geog": "gcs_north_american_1927",
        "code": 32045,
    },
    {
        "proj": "spcs27__virginia_north",
        "geog": "gcs_north_american_1927",
        "code": 32046,
    },
    {
        "proj": "spcs27__virginia_south",
        "geog": "gcs_north_american_1927",
        "code": 32047,
    },
    {
        "proj": "spcs27__washington_north",
        "geog": "gcs_north_american_1927",
        "code": 32048,
    },
    {
        "proj": "spcs27__washington_south",
        "geog": "gcs_north_american_1927",
        "code": 32049,
    },
    {
        "proj": "spcs27__west_virginia_north",
        "geog": "gcs_north_american_1927",
        "code": 32050,
    },
    {
        "proj": "spcs27__west_virginia_south",
        "geog": "gcs_north_american_1927",
        "code": 32051,
    },
    {
        "proj": "spcs27__wisconsin_central",
        "geog": "gcs_north_american_1927",
        "code": 32053,
    },
    {
        "proj": "spcs27__wisconsin_north",
        "geog": "gcs_north_american_1927",
        "code": 32052,
    },
    {
        "proj": "spcs27__wisconsin_south",
        "geog": "gcs_north_american_1927",
        "code": 32054,
    },
    {
        "proj": "spcs27__wyoming_east",
        "geog": "gcs_north_american_1927",
        "code": 32055,
    },
    {
        "proj": "spcs27__wyoming_east_central",
        "geog": "gcs_north_american_1927",
        "code": 32056,
    },
    {
        "proj": "spcs27__wyoming_west",
        "geog": "gcs_north_american_1927",
        "code": 32058,
    },
    {
        "proj": "spcs27__wyoming_west_central",
        "geog": "gcs_north_american_1927",
        "code": 32057,
    },
    {
        "proj": "utm_zone_1_north_180_w__174_w_longitude",
        "geog": "gcs_north_american_1927",
        "code": 26701,
    },
    {
        "proj": "utm_zone_2_north_174_w__168_w_longitude",
        "geog": "gcs_north_american_1927",
        "code": 26702,
    },
    {
        "proj": "utm_zone_3_north_168_w__162_w_longitude",
        "geog": "gcs_north_american_1927",
        "code": 26703,
    },
    {
        "proj": "utm_zone_4_north_162_w__156_w_longitude",
        "geog": "gcs_north_american_1927",
        "code": 26704,
    },
    {
        "proj": "utm_zone_5_north_156_w__150_w_longitude",
        "geog": "gcs_north_american_1927",
        "code": 26705,
    },
    {
        "proj": "utm_zone_6_north_150_w__144_w_longitude",
        "geog": "gcs_north_american_1927",
        "code": 26706,
    },
    {
        "proj": "utm_zone_7_north_144_w__138_w_longitude",
        "geog": "gcs_north_american_1927",
        "code": 26707,
    },
    {
        "proj": "utm_zone_8_north_138_w__132_w_longitude",
        "geog": "gcs_north_american_1927",
        "code": 26708,
    },
    {
        "proj": "utm_zone_9_north_132_w__126_w_longitude",
        "geog": "gcs_north_american_1927",
        "code": 26709,
    },
    {
        "proj": "utm_zone_10_north_126_w__120_w_longitude",
        "geog": "gcs_north_american_1927",
        "code": 26710,
    },
    {
        "proj": "utm_zone_11_north_120_w__114_w_longitude",
        "geog": "gcs_north_american_1927",
        "code": 26711,
    },
    {
        "proj": "utm_zone_12_north_114_w__108_w_longitude",
        "geog": "gcs_north_american_1927",
        "code": 26712,
    },
    {
        "proj": "utm_zone_13_north_108_w__102_w_longitude",
        "geog": "gcs_north_american_1927",
        "code": 26713,
    },
    {
        "proj": "utm_zone_14_north_102_w__96_w_longitude",
        "geog": "gcs_north_american_1927",
        "code": 26714,
    },
    {
        "proj": "utm_zone_15_north_96_w__90_w_longitude",
        "geog": "gcs_north_american_1927",
        "code": 26715,
    },
    {
        "proj": "utm_zone_16_north_90_w__84_w_longitude",
        "geog": "gcs_north_american_1927",
        "code": 26716,
    },
    {
        "proj": "utm_zone_17_north_84_w__78_w_longitude",
        "geog": "gcs_north_american_1927",
        "code": 26717,
    },
    {
        "proj": "utm_zone_18_north_78_w__72_w_longitude",
        "geog": "gcs_north_american_1927",
        "code": 26718,
    },
    {
        "proj": "utm_zone_19_north_72_w__66_w_longitude",
        "geog": "gcs_north_american_1927",
        "code": 26719,
    },
    {
        "proj": "utm_zone_1__u_s__survey_feet_180_w__174_w_longitude",
        "geog": "gcs_north_american_1927",
        "code": 4401,
    },
    {
        "proj": "utm_zone_2__u_s__survey_feet_174_w__168_w_longitude",
        "geog": "gcs_north_american_1927",
        "code": 4402,
    },
    {
        "proj": "utm_zone_3__u_s__survey_feet_168_w__162_w_longitude",
        "geog": "gcs_north_american_1927",
        "code": 4403,
    },
    {
        "proj": "utm_zone_4__u_s__survey_feet_162_w__156_w_longitude",
        "geog": "gcs_north_american_1927",
        "code": 4404,
    },
    {
        "proj": "utm_zone_5__u_s__survey_feet_156_w__150_w_longitude",
        "geog": "gcs_north_american_1927",
        "code": 4405,
    },
    {
        "proj": "utm_zone_6__u_s__survey_feet_150_w__144_w_longitude",
        "geog": "gcs_north_american_1927",
        "code": 4406,
    },
    {
        "proj": "utm_zone_7__u_s__survey_feet_144_w__138_w_longitude",
        "geog": "gcs_north_american_1927",
        "code": 4407,
    },
    {
        "proj": "utm_zone_8__u_s__survey_feet_138_w__132_w_longitude",
        "geog": "gcs_north_american_1927",
        "code": 4408,
    },
    {
        "proj": "utm_zone_9__u_s__survey_feet_132_w__126_w_longitude",
        "geog": "gcs_north_american_1927",
        "code": 4409,
    },
    {
        "proj": "utm_zone_10__u_s__survey_feet_126_w__120_w_longitude",
        "geog": "gcs_north_american_1927",
        "code": 4410,
    },
    {
        "proj": "utm_zone_11__u_s__survey_feet_120_w__114_w_longitude",
        "geog": "gcs_north_american_1927",
        "code": 4411,
    },
    {
        "proj": "utm_zone_12__u_s__survey_feet_114_w__108_w_longitude",
        "geog": "gcs_north_american_1927",
        "code": 4412,
    },
    {
        "proj": "utm_zone_13__u_s__survey_feet_108_w__102_w_longitude",
        "geog": "gcs_north_american_1927",
        "code": 4413,
    },
    {
        "proj": "utm_zone_14__u_s__survey_feet_102_w__96_w_longitude",
        "geog": "gcs_north_american_1927",
        "code": 4414,
    },
    {
        "proj": "utm_zone_15__u_s__survey_feet_96_w__90_w_longitude",
        "geog": "gcs_north_american_1927",
        "code": 4415,
    },
    {
        "proj": "utm_zone_16__u_s__survey_feet_90_w__84_w_longitude",
        "geog": "gcs_north_american_1927",
        "code": 4416,
    },
    {
        "proj": "utm_zone_17__u_s__survey_feet_84_w__78_w_longitude",
        "geog": "gcs_north_american_1927",
        "code": 4417,
    },
    {
        "proj": "utm_zone_18__u_s__survey_feet_78_w__72_w_longitude",
        "geog": "gcs_north_american_1927",
        "code": 4418,
    },
    {
        "proj": "utm_zone_19__u_s__survey_feet_72_w__66_w_longitude",
        "geog": "gcs_north_american_1927",
        "code": 4419,
    },
    {
        "proj": "state_plane_1927__alaska_zone_1",
        "geog": "gcs_north_american_1927",
        "code": 26731,
    },
    {
        "proj": "state_plane_1927__alaska_zone_2",
        "geog": "gcs_north_american_1927",
        "code": 26732,
    },
    {
        "proj": "state_plane_1927__alaska_zone_3",
        "geog": "gcs_north_american_1927",
        "code": 26733,
    },
    {
        "proj": "state_plane_1927__alaska_zone_4",
        "geog": "gcs_north_american_1927",
        "code": 26734,
    },
    {
        "proj": "state_plane_1927__alaska_zone_5",
        "geog": "gcs_north_american_1927",
        "code": 26735,
    },
    {
        "proj": "state_plane_1927__alaska_zone_6",
        "geog": "gcs_north_american_1927",
        "code": 26736,
    },
    {
        "proj": "state_plane_1927__alaska_zone_7",
        "geog": "gcs_north_american_1927",
        "code": 26737,
    },
    {
        "proj": "state_plane_1927__alaska_zone_8",
        "geog": "gcs_north_american_1927",
        "code": 26738,
    },
    {
        "proj": "state_plane_1927__alaska_zone_9",
        "geog": "gcs_north_american_1927",
        "code": 26739,
    },
    {
        "proj": "state_plane_1927__alaska_zone_10",
        "geog": "gcs_north_american_1927",
        "code": 26740,
    },
    {
        "proj": "spcs83__alabama_east",
        "geog": "gcs_north_american_1983",
        "code": 26929,
    },
    {
        "proj": "spcs83__alabama_west",
        "geog": "gcs_north_american_1983",
        "code": 26930,
    },
    {
        "proj": "spcs83__arizona_central",
        "geog": "gcs_north_american_1983",
        "code": 26949,
    },
    {
        "proj": "spcs83__arizona_east",
        "geog": "gcs_north_american_1983",
        "code": 26948,
    },
    {
        "proj": "spcs83__arizona_west",
        "geog": "gcs_north_american_1983",
        "code": 26950,
    },
    {
        "proj": "spcs83__arkansas_north",
        "geog": "gcs_north_american_1983",
        "code": 26951,
    },
    {
        "proj": "spcs83__arkansas_south",
        "geog": "gcs_north_american_1983",
        "code": 26952,
    },
    {
        "proj": "spcs83__california_i",
        "geog": "gcs_north_american_1983",
        "code": 26941,
    },
    {
        "proj": "spcs83__california_ii",
        "geog": "gcs_north_american_1983",
        "code": 26942,
    },
    {
        "proj": "spcs83__california_iii",
        "geog": "gcs_north_american_1983",
        "code": 26943,
    },
    {
        "proj": "spcs83__california_iv",
        "geog": "gcs_north_american_1983",
        "code": 26944,
    },
    {
        "proj": "spcs83__california_v",
        "geog": "gcs_north_american_1983",
        "code": 26945,
    },
    {
        "proj": "spcs83__california_vi",
        "geog": "gcs_north_american_1983",
        "code": 26946,
    },
    {
        "proj": "spcs83__colorado_central",
        "geog": "gcs_north_american_1983",
        "code": 26954,
    },
    {
        "proj": "spcs83__colorado_north",
        "geog": "gcs_north_american_1983",
        "code": 26953,
    },
    {
        "proj": "spcs83__colorado_south",
        "geog": "gcs_north_american_1983",
        "code": 26955,
    },
    {
        "proj": "spcs83__connecticut",
        "geog": "gcs_north_american_1983",
        "code": 26956,
    },
    {
        "proj": "spcs83__delaware",
        "geog": "gcs_north_american_1983",
        "code": 26957,
    },
    {
        "proj": "spcs83__florida_east",
        "geog": "gcs_north_american_1983",
        "code": 26958,
    },
    {
        "proj": "spcs83__florida_north",
        "geog": "gcs_north_american_1983",
        "code": 26960,
    },
    {
        "proj": "spcs83__florida_west",
        "geog": "gcs_north_american_1983",
        "code": 26959,
    },
    {
        "proj": "spcs83__georgia_east",
        "geog": "gcs_north_american_1983",
        "code": 26966,
    },
    {
        "proj": "spcs83__georgia_west",
        "geog": "gcs_north_american_1983",
        "code": 26967,
    },
    {
        "proj": "spcs83__idaho_central",
        "geog": "gcs_north_american_1927",
        "code": 26969,
    },
    {
        "proj": "spcs83__idaho_east",
        "geog": "gcs_north_american_1927",
        "code": 26968,
    },
    {
        "proj": "spcs83__idaho_west",
        "geog": "gcs_north_american_1927",
        "code": 26970,
    },
    {
        "proj": "spcs83__illinois_east",
        "geog": "gcs_north_american_1983",
        "code": 26971,
    },
    {
        "proj": "spcs83__illinois_west",
        "geog": "gcs_north_american_1983",
        "code": 26972,
    },
    {
        "proj": "spcs83__indiana_east",
        "geog": "gcs_north_american_1983",
        "code": 26973,
    },
    {
        "proj": "spcs83__indiana_west",
        "geog": "gcs_north_american_1983",
        "code": 26974,
    },
    {
        "proj": "spcs83__iowa_north",
        "geog": "gcs_north_american_1983",
        "code": 26975,
    },
    {
        "proj": "spcs83__iowa_south",
        "geog": "gcs_north_american_1983",
        "code": 26976,
    },
    {
        "proj": "spcs83__kansas_north",
        "geog": "gcs_north_american_1983",
        "code": 26977,
    },
    {
        "proj": "spcs83__kansas_south",
        "geog": "gcs_north_american_1983",
        "code": 26978,
    },
    {
        "proj": "spcs83__kentucky_north",
        "geog": "gcs_north_american_1983",
        "code": 2205,
    },
    {
        "proj": "spcs83__kentucky_south",
        "geog": "gcs_north_american_1983",
        "code": 26980,
    },
    {
        "proj": "spcs83__louisiana_north",
        "geog": "gcs_north_american_1983",
        "code": 26981,
    },
    {
        "proj": "spcs83__louisiana_offshore",
        "geog": "gcs_north_american_1983",
        "code": 32199,
    },
    {
        "proj": "spcs83__louisiana_south",
        "geog": "gcs_north_american_1983",
        "code": 26982,
    },
    {
        "proj": "spcs83__maine_east_gcs",
        "geog": "north_american_1983",
        "code": 26983,
    },
    {
        "proj": "spcs83__maine_west_gcs",
        "geog": "north_american_1983",
        "code": 26984,
    },
    {
        "proj": "spcs83__maryland_gcs",
        "geog": "north_american_1983",
        "code": 26985,
    },
    {
        "proj": "spcs83__massachusetts_island",
        "geog": "gcs_north_american_1983",
        "code": 26987,
    },
    {
        "proj": "spcs83__massachusetts_mainland",
        "geog": "gcs_north_american_1983",
        "code": 26986,
    },
    {
        "proj": "spcs83__michigan_central_lambert",
        "geog": "gcs_north_american_1983",
        "code": 26989,
    },
    {
        "proj": "spcs83__michigan_north_lambert",
        "geog": "gcs_north_american_1983",
        "code": 26988,
    },
    {
        "proj": "spcs83__michigan_south_lambert",
        "geog": "gcs_north_american_1983",
        "code": 26990,
    },
    {
        "proj": "spcs83__minnesota_central",
        "geog": "gcs_north_american_1983",
        "code": 26992,
    },
    {
        "proj": "spcs83__minnesota_north",
        "geog": "gcs_north_american_1983",
        "code": 26991,
    },
    {
        "proj": "spcs83__minnesota_south",
        "geog": "gcs_north_american_1983",
        "code": 26993,
    },
    {
        "proj": "spcs83__mississippi_east",
        "geog": "gcs_north_american_1983",
        "code": 26994,
    },
    {
        "proj": "spcs83__mississippi_west",
        "geog": "gcs_north_american_1983",
        "code": 26995,
    },
    {
        "proj": "spcs83__missouri_central",
        "geog": "gcs_north_american_1983",
        "code": 26997,
    },
    {
        "proj": "spcs83__missouri_east",
        "geog": "gcs_north_american_1983",
        "code": 26996,
    },
    {
        "proj": "spcs83__missouri_west",
        "geog": "gcs_north_american_1983",
        "code": 26998,
    },
    {
        "proj": "spcs83__montana",
        "geog": "gcs_north_american_1983",
        "code": 32100,
    },
    {
        "proj": "spcs83__nebraska",
        "geog": "gcs_north_american_1983",
        "code": 32104,
    },
    {
        "proj": "spcs83__nevada_central",
        "geog": "gcs_north_american_1983",
        "code": 32108,
    },
    {
        "proj": "spcs83__nevada_east",
        "geog": "gcs_north_american_1983",
        "code": 32107,
    },
    {
        "proj": "spcs83__nevada_west",
        "geog": "gcs_north_american_1983",
        "code": 32109,
    },
    {
        "proj": "spcs83__new_hampshire",
        "geog": "gcs_north_american_1983",
        "code": 32110,
    },
    {
        "proj": "spcs83__new_jersey",
        "geog": "gcs_north_american_1983",
        "code": 32111,
    },
    {
        "proj": "spcs83__new_mexico_central",
        "geog": "gcs_north_american_1983",
        "code": 32113,
    },
    {
        "proj": "spcs83__new_mexico_east",
        "geog": "gcs_north_american_1983",
        "code": 32112,
    },
    {
        "proj": "spcs83__new_mexico_west",
        "geog": "gcs_north_american_1983",
        "code": 32114,
    },
    {
        "proj": "spcs83__new_york_central",
        "geog": "gcs_north_american_1983",
        "code": 32116,
    },
    {
        "proj": "spcs83__new_york_east",
        "geog": "gcs_north_american_1983",
        "code": 32115,
    },
    {
        "proj": "spcs83__new_york_long_island",
        "geog": "gcs_north_american_1983",
        "code": 32118,
    },
    {
        "proj": "spcs83__new_york_west",
        "geog": "gcs_north_american_1983",
        "code": 32117,
    },
    {
        "proj": "spcs83__north_carolina",
        "geog": "gcs_north_american_1983",
        "code": 32119,
    },
    {
        "proj": "spcs83__north_dakota_north",
        "geog": "gcs_north_american_1983",
        "code": 32120,
    },
    {
        "proj": "spcs83__north_dakota_south",
        "geog": "gcs_north_american_1983",
        "code": 32121,
    },
    {
        "proj": "spcs83__ohio_north",
        "geog": "gcs_north_american_1983",
        "code": 32122,
    },
    {
        "proj": "spcs83__ohio_south",
        "geog": "gcs_north_american_1983",
        "code": 32123,
    },
    {
        "proj": "spcs83__oklahoma_north",
        "geog": "gcs_north_american_1983",
        "code": 32124,
    },
    {
        "proj": "spcs83__oklahoma_south",
        "geog": "gcs_north_american_1983",
        "code": 32125,
    },
    {
        "proj": "spcs83__oregon_north",
        "geog": "gcs_north_american_1983",
        "code": 32126,
    },
    {
        "proj": "spcs83__oregon_south",
        "geog": "gcs_north_american_1983",
        "code": 32127,
    },
    {
        "proj": "spcs83__pennsylvania_north",
        "geog": "gcs_north_american_1983",
        "code": 32128,
    },
    {
        "proj": "spcs83__pennsylvania_south",
        "geog": "gcs_north_american_1983",
        "code": 32129,
    },
    {
        "proj": "spcs83__rhode_island_south",
        "geog": "gcs_north_american_1983",
        "code": 32130,
    },
    {
        "proj": "spcs83__south_carolina",
        "geog": "gcs_north_american_1983",
        "code": 32133,
    },
    {
        "proj": "spcs83__south_dakota_north",
        "geog": "gcs_north_american_1983",
        "code": 32134,
    },
    {
        "proj": "spcs83__south_dakota_south",
        "geog": "gcs_north_american_1983",
        "code": 32125,
    },
    {
        "proj": "spcs83__tennessee",
        "geog": "gcs_north_american_1983",
        "code": 32136,
    },
    {
        "proj": "spcs83__texas_central",
        "geog": "gcs_north_american_1983",
        "code": 32139,
    },
    {
        "proj": "spcs83__texas_north",
        "geog": "gcs_north_american_1983",
        "code": 32137,
    },
    {
        "proj": "spcs83__texas_north_central",
        "geog": "gcs_north_american_1983",
        "code": 32138,
    },
    {
        "proj": "spcs83__texas_south",
        "geog": "gcs_north_american_1983",
        "code": 32141,
    },
    {
        "proj": "spcs83__texas_south_central",
        "geog": "gcs_north_american_1983",
        "code": 32140,
    },
    {
        "proj": "spcs83__utah_central",
        "geog": "gcs_north_american_1983",
        "code": 32143,
    },
    {
        "proj": "spcs83__utah_north",
        "geog": "gcs_north_american_1983",
        "code": 32142,
    },
    {
        "proj": "spcs83__utah_south",
        "geog": "gcs_north_american_1983",
        "code": 32144,
    },
    {
        "proj": "spcs83__vermont",
        "geog": "gcs_north_american_1983",
        "code": 32145,
    },
    {
        "proj": "spcs83__virginia_north",
        "geog": "gcs_north_american_1983",
        "code": 32146,
    },
    {
        "proj": "spcs83__virginia_south",
        "geog": "gcs_north_american_1983",
        "code": 32147,
    },
    {
        "proj": "spcs83__washington_north",
        "geog": "gcs_north_american_1983",
        "code": 32148,
    },
    {
        "proj": "spcs83__washington_south",
        "geog": "gcs_north_american_1983",
        "code": 32149,
    },
    {
        "proj": "spcs83__west_virginia_north",
        "geog": "gcs_north_american_1983",
        "code": 32150,
    },
    {
        "proj": "spcs83__west_virginia_south",
        "geog": "gcs_north_american_1983",
        "code": 32151,
    },
    {
        "proj": "spcs83__wisconsin_central",
        "geog": "gcs_north_american_1983",
        "code": 32153,
    },
    {
        "proj": "spcs83__wisconsin_north",
        "geog": "gcs_north_american_1983",
        "code": 32152,
    },
    {
        "proj": "spcs83__wisconsin_south",
        "geog": "gcs_north_american_1983",
        "code": 32154,
    },
    {
        "proj": "spcs83__wyoming_east",
        "geog": "gcs_north_american_1983",
        "code": 32155,
    },
    {
        "proj": "spcs83__wyoming_east_central",
        "geog": "gcs_north_american_1983",
        "code": 32156,
    },
    {
        "proj": "spcs83__wyoming_west",
        "geog": "gcs_north_american_1983",
        "code": 32158,
    },
    {
        "proj": "spcs83__wyoming_west_central",
        "geog": "gcs_north_american_1983",
        "code": 32157,
    },
    {
        "proj": "utm_zone_1_north_180_w__174_w_longitude",
        "geog": "gcs_north_american_1983",
        "code": 26901,
    },
    {
        "proj": "utm_zone_2_north_174_w__168_w_longitude",
        "geog": "gcs_north_american_1983",
        "code": 26902,
    },
    {
        "proj": "utm_zone_3_north_168_w__162_w_longitude",
        "geog": "gcs_north_american_1983",
        "code": 26903,
    },
    {
        "proj": "utm_zone_4_north_162_w__156_w_longitude",
        "geog": "gcs_north_american_1983",
        "code": 26904,
    },
    {
        "proj": "utm_zone_5_north_156_w__150_w_longitude",
        "geog": "gcs_north_american_1983",
        "code": 26905,
    },
    {
        "proj": "utm_zone_6_north_150_w__144_w_longitude",
        "geog": "gcs_north_american_1983",
        "code": 26906,
    },
    {
        "proj": "utm_zone_7_north_144_w__138_w_longitude",
        "geog": "gcs_north_american_1983",
        "code": 26907,
    },
    {
        "proj": "utm_zone_8_north_138_w__132_w_longitude",
        "geog": "gcs_north_american_1983",
        "code": 26908,
    },
    {
        "proj": "utm_zone_9_north_132_w__126_w_longitude",
        "geog": "gcs_north_american_1983",
        "code": 26909,
    },
    {
        "proj": "utm_zone_10_north_126_w__120_w_longitude",
        "geog": "gcs_north_american_1983",
        "code": 26910,
    },
    {
        "proj": "utm_zone_11_north_120_w__114_w_longitude",
        "geog": "gcs_north_american_1983",
        "code": 26911,
    },
    {
        "proj": "utm_zone_12_north_114_w__108_w_longitude",
        "geog": "gcs_north_american_1983",
        "code": 26912,
    },
    {
        "proj": "utm_zone_13_north_108_w__102_w_longitude",
        "geog": "gcs_north_american_1983",
        "code": 26913,
    },
    {
        "proj": "utm_zone_14_north_102_w__96_w_longitude",
        "geog": "gcs_north_american_1983",
        "code": 26914,
    },
    {
        "proj": "utm_zone_15_north_96_w__90_w_longitude",
        "geog": "gcs_north_american_1983",
        "code": 26915,
    },
    {
        "proj": "utm_zone_16_north_90_w__84_w_longitude",
        "geog": "gcs_north_american_1983",
        "code": 26916,
    },
    {
        "proj": "utm_zone_17_north_84_w__78_w_longitude",
        "geog": "gcs_north_american_1983",
        "code": 26917,
    },
    {
        "proj": "utm_zone_18_north_78_w__72_w_longitude",
        "geog": "gcs_north_american_1983",
        "code": 26918,
    },
    {
        "proj": "utm_zone_19_north_72_w__66_w_longitude",
        "geog": "gcs_north_american_1983",
        "code": 26919,
    },
    {
        "proj": "utm_zone_1__u_s__survey_feet_180_w__174_w_longitude",
        "geog": "gcs_north_american_1983",
        "code": 4421,
    },
    {
        "proj": "utm_zone_2__u_s__survey_feet_174_w__168_w_longitude",
        "geog": "gcs_north_american_1983",
        "code": 4422,
    },
    {
        "proj": "utm_zone_3__u_s__survey_feet_168_w__162_w_longitude",
        "geog": "gcs_north_american_1983",
        "code": 4423,
    },
    {
        "proj": "utm_zone_4__u_s__survey_feet_162_w__156_w_longitude",
        "geog": "gcs_north_american_1983",
        "code": 4424,
    },
    {
        "proj": "utm_zone_5__u_s__survey_feet_156_w__150_w_longitude",
        "geog": "gcs_north_american_1983",
        "code": 4425,
    },
    {
        "proj": "utm_zone_6__u_s__survey_feet_150_w__144_w_longitude",
        "geog": "gcs_north_american_1983",
        "code": 4426,
    },
    {
        "proj": "utm_zone_7__u_s__survey_feet_144_w__138_w_longitude",
        "geog": "gcs_north_american_1983",
        "code": 4427,
    },
    {
        "proj": "utm_zone_8__u_s__survey_feet_138_w__132_w_longitude",
        "geog": "gcs_north_american_1983",
        "code": 4428,
    },
    {
        "proj": "utm_zone_9__u_s__survey_feet_132_w__126_w_longitude",
        "geog": "gcs_north_american_1983",
        "code": 4429,
    },
    {
        "proj": "utm_zone_10__u_s__survey_feet_126_w__120_w_longitude",
        "geog": "gcs_north_american_1983",
        "code": 4430,
    },
    {
        "proj": "utm_zone_11__u_s__survey_feet_120_w__114_w_longitude",
        "geog": "gcs_north_american_1983",
        "code": 4431,
    },
    {
        "proj": "utm_zone_12__u_s__survey_feet_114_w__108_w_longitude",
        "geog": "gcs_north_american_1983",
        "code": 4432,
    },
    {
        "proj": "utm_zone_13__u_s__survey_feet_108_w__102_w_longitude",
        "geog": "gcs_north_american_1983",
        "code": 4433,
    },
    {
        "proj": "utm_zone_14__u_s__survey_feet_102_w__96_w_longitude",
        "geog": "gcs_north_american_1983",
        "code": 4434,
    },
    {
        "proj": "utm_zone_15__u_s__survey_feet_96_w__90_w_longitude",
        "geog": "gcs_north_american_1983",
        "code": 4435,
    },
    {
        "proj": "utm_zone_16__u_s__survey_feet_90_w__84_w_longitude",
        "geog": "gcs_north_american_1983",
        "code": 4436,
    },
    {
        "proj": "utm_zone_17__u_s__survey_feet_84_w__78_w_longitude",
        "geog": "gcs_north_american_1983",
        "code": 4437,
    },
    {
        "proj": "utm_zone_18__u_s__survey_feet_78_w__72_w_longitude",
        "geog": "gcs_north_american_1983",
        "code": 4438,
    },
    {
        "proj": "utm_zone_19__u_s__survey_feet_72_w__66_w_longitude",
        "geog": "gcs_north_american_1983",
        "code": 4439,
    },
    {
        "proj": "state_plane_1983__alaska_zone_1",
        "geog": "gcs_north_american_1983",
        "code": 26931,
    },
    {
        "proj": "state_plane_1983__alaska_zone_2",
        "geog": "gcs_north_american_1983",
        "code": 26932,
    },
    {
        "proj": "state_plane_1983__alaska_zone_3",
        "geog": "gcs_north_american_1983",
        "code": 26933,
    },
    {
        "proj": "state_plane_1983__alaska_zone_4",
        "geog": "gcs_north_american_1983",
        "code": 26934,
    },
    {
        "proj": "state_plane_1983__alaska_zone_5",
        "geog": "gcs_north_american_1983",
        "code": 26935,
    },
    {
        "proj": "state_plane_1983__alaska_zone_6",
        "geog": "gcs_north_american_1983",
        "code": 26936,
    },
    {
        "proj": "state_plane_1983__alaska_zone_7",
        "geog": "gcs_north_american_1983",
        "code": 26937,
    },
    {
        "proj": "state_plane_1983__alaska_zone_8",
        "geog": "gcs_north_american_1983",
        "code": 26938,
    },
    {
        "proj": "state_plane_1983__alaska_zone_9",
        "geog": "gcs_north_american_1983",
        "code": 26939,
    },
    {
        "proj": "state_plane_1983__alaska_zone_10",
        "geog": "gcs_north_american_1983",
        "code": 26940,
    },
    {
        "proj": "utm_zone_1_north_180_w__174_w_longitude",
        "geog": "gcs_wgs_1984",
        "code": 32601,
    },
    {
        "proj": "utm_zone_2_north_174_w__168_w_longitude",
        "geog": "gcs_wgs_1984",
        "code": 32602,
    },
    {
        "proj": "utm_zone_3_north_168_w__162_w_longitude",
        "geog": "gcs_wgs_1984",
        "code": 32603,
    },
    {
        "proj": "utm_zone_4_north_162_w__156_w_longitude",
        "geog": "gcs_wgs_1984",
        "code": 32604,
    },
    {
        "proj": "utm_zone_5_north_156_w__150_w_longitude",
        "geog": "gcs_wgs_1984",
        "code": 32605,
    },
    {
        "proj": "utm_zone_6_north_150_w__144_w_longitude",
        "geog": "gcs_wgs_1984",
        "code": 32606,
    },
    {
        "proj": "utm_zone_7_north_144_w__138_w_longitude",
        "geog": "gcs_wgs_1984",
        "code": 32607,
    },
    {
        "proj": "utm_zone_8_north_138_w__132_w_longitude",
        "geog": "gcs_wgs_1984",
        "code": 32608,
    },
    {
        "proj": "utm_zone_9_north_132_w__126_w_longitude",
        "geog": "gcs_wgs_1984",
        "code": 32609,
    },
    {
        "proj": "utm_zone_10_north_126_w__120_w_longitude",
        "geog": "gcs_wgs_1984",
        "code": 32610,
    },
    {
        "proj": "utm_zone_11_north_120_w__114_w_longitude",
        "geog": "gcs_wgs_1984",
        "code": 32611,
    },
    {
        "proj": "utm_zone_12_north_114_w__108_w_longitude",
        "geog": "gcs_wgs_1984",
        "code": 32612,
    },
    {
        "proj": "utm_zone_13_north_108_w__102_w_longitude",
        "geog": "gcs_wgs_1984",
        "code": 32613,
    },
    {
        "proj": "utm_zone_14_north_102_w__96_w_longitude",
        "geog": "gcs_wgs_1984",
        "code": 32614,
    },
    {
        "proj": "utm_zone_15_north_96_w__90_w_longitude",
        "geog": "gcs_wgs_1984",
        "code": 32615,
    },
    {
        "proj": "utm_zone_16_north_90_w__84_w_longitude",
        "geog": "gcs_wgs_1984",
        "code": 32616,
    },
    {
        "proj": "utm_zone_17_north_84_w__78_w_longitude",
        "geog": "gcs_wgs_1984",
        "code": 32617,
    },
    {
        "proj": "utm_zone_18_north_78_w__72_w_longitude",
        "geog": "gcs_wgs_1984",
        "code": 32618,
    },
    {
        "proj": "utm_zone_19_north_72_w__66_w_longitude",
        "geog": "gcs_wgs_1984",
        "code": 32619,
    },
    {
        "proj": "albers_equal_area_conic_usgs_dlg_1_to_2m",
        "geog": "gcs_north_american_1983",
        "code": 102008,
    },
    {
        "proj": "albers_equal_area_conic_usgs_dlg_1_to_2m",
        "geog": "gcs_north_american_1927",
        "code": 5069,
    },
    {
        "proj": "world__mercator",
        "geog": "gcs_north_american_1983",
        "code": 3395,
    },
    {"proj": "new_brunswick", "geog": "gcs_north_american_1927", "code": 5588},
    {
        "proj": "prince_edward_island",
        "geog": "gcs_north_american_1927",
        "code": 2290,
    },
]


def scrub(s: str) -> str:
    underscored = re.sub(r"[ ./_]", "_", str(s))
    return re.sub(r"[-()]", "", underscored)


def get_wkts(fs_path: str):
    """doc"""
    ggx_xml = os.path.join(fs_path, "Project.ggx.xml")
    root = ET.parse(ggx_xml).getroot()

    storage_wkt = root.find("./Project/StorageCoordinateSystem/ESRI").text
    display_wkt = root.find("./Project/DisplayCoordinateSystem/ESRI").text

    return {
        "storage_wkt": scrub(storage_wkt),
        "display_wkt": scrub(display_wkt),
    }


def epsg_codes(repo):
    """doc"""

    storage_epsg = 0
    storage_name = "unknown"
    display_epsg = 0
    display_name = "unknown"

    wkt = get_wkts(repo.get("fs_path"))

    # s_geog = re.search(r"(?<=GEOGCS\[\").+?(?=\"\,)", wkt.get("storage_wkt"))
    s_geog = re.search(r"(?<=GEOGCS\[\").+?(?=\",)", wkt.get("storage_wkt"))
    stor_geog = s_geog[0].lower() if s_geog is not None else None

    s_datum = re.search(r"(?<=DATUM\[\").+?(?=\",)", wkt.get("storage_wkt"))
    stor_datum = s_datum[0].lower() if s_datum is not None else None

    d_geog = re.search(r"(?<=GEOGCS\[\").+?(?=\",)", wkt.get("display_wkt"))
    disp_geog = d_geog[0].lower() if d_geog is not None else None

    d_proj = re.search(r"(?<=PROJCS\[\").+?(?=\",)", wkt.get("display_wkt"))
    disp_proj = d_proj[0].lower() if d_proj is not None else None

    for o in [
        x
        for x in geodetics
        if (x.get("geog") == stor_geog and x.get("datum") == stor_datum)
    ]:
        storage_epsg = o.get("code")
        storage_name = o.get("geog")

    for o in [
        x
        for x in projections
        if (x.get("geog") == disp_geog and x.get("proj") == disp_proj)
    ]:
        display_epsg = o.get("code")
        display_name = o.get("proj")

    return {
        "storage_epsg": storage_epsg,
        "storage_name": storage_name,
        "display_epsg": display_epsg,
        "display_name": display_name,
    }
