class MyCollection:
    def __init__(self):
        self._data = {"a":12, "b":34, "c":54}

    def __iter__(self):
        '''
        Return an iterator of contained values
        '''
        return iter(self._data.values())

a = MyCollection()

import itertools
def iter_safe(value):
    if isinstance(value, str):
        value = (value,)
    try:
        iter(value)
    except TypeError:
        return (value,)
    else:
        return value

def dict_combinations(d):
    keys, values_list = zip(*d.items())
    for values in itertools.product(*map(iter_safe, values_list)):
        yield dict(zip(keys, values))

print dict_combinations({"a":12, "b":34, "c":54})

c = [{
            "is_general": True,
            "p0_posterior": 0.35102611434908654,
            "rank": 1,
            "topic_id": "22596701"
        },
        {
            "is_general": True,
            "p0_posterior": 0.19113380243996972,
            "rank": 2,
            "topic_id": "22553543"
        },
        {
            "is_general": True,
            "p0_posterior": 0.07427965147927013,
            "rank": 3,
            "topic_id": "22553321"
        },
        {
            "is_general": False,
            "p0_posterior": 0.04810021534759652,
            "rank": 1,
            "topic_id": "27427842"
        },
        {
            "is_general": False,
            "p0_posterior": 0.013607696872596711,
            "rank": 3,
            "topic_id": "1039929544"
        },
        {
            "is_general": False,
            "p0_posterior": 0.007692916483195053,
            "rank": 5,
            "topic_id": "234485158"
        },
        {
            "is_general": False,
            "p0_posterior": 0.006677534534244367,
            "rank": 5,
            "topic_id": "70158140"
        },
        {
            "is_general": False,
            "p0_posterior": 0.006169281157163513,
            "rank": 5,
            "topic_id": "922928822"
        },
        {
            "is_general": False,
            "p0_posterior": 0.00592623468892879,
            "rank": 6,
            "topic_id": "1017598920"
        },
        {
            "is_general": False,
            "p0_posterior": 0.0019496378363475567,
            "rank": 6,
            "topic_id": "1039931741"
        },
        {
            "is_general": False,
            "p0_posterior": 0.004937434561876108,
            "rank": 7,
            "topic_id": "27428150"
        },
        {
            "is_general": False,
            "p0_posterior": 0.003517203635076847,
            "rank": 8,
            "topic_id": "103112822"
        },
        {
            "is_general": False,
            "p0_posterior": 0.0025606215649940215,
            "rank": 9,
            "topic_id": "1033976850"
        },
        {
            "is_general": False,
            "p0_posterior": 0.0015013153916703776,
            "rank": 9,
            "topic_id": "27435843"
        },
        {
            "is_general": False,
            "p0_posterior": 0.00010609311639798023,
            "rank": 11,
            "topic_id": "315971729"
        },
        {
            "is_general": False,
            "p0_posterior": 0.0015074750282439713,
            "rank": 12,
            "topic_id": "116192885"
        },
        {
            "is_general": False,
            "p0_posterior": 0.0015063411572629087,
            "rank": 12,
            "topic_id": "46860489"
        },
        {
            "is_general": False,
            "p0_posterior": 0.00025310919767755266,
            "rank": 12.5,
            "topic_id": "1153773792"
        },
        {
            "is_general": False,
            "p0_posterior": 0.0012433337806999952,
            "rank": 13,
            "topic_id": "671270912"
        },
        {
            "is_general": False,
            "p0_posterior": 0.0011678184759830656,
            "rank": 14,
            "topic_id": "27427777"
        },
        {
            "is_general": False,
            "p0_posterior": 0.000831169292708897,
            "rank": 15,
            "topic_id": "600986675"
        }].__iter__()

d = [
        {
            "topic_name": "Marketing",
            "interest_score": 0.0000154796,
            "rank": 1,
            "topic_id": "24494375"
        },
        {
            "topic_name": "Modifikasi",
            "interest_score": 0.000041949,
            "rank": 1,
            "topic_id": "1035897075"
        },
        {
            "topic_name": "Singapura",
            "interest_score": 0.0000463568,
            "rank": 1,
            "topic_id": "46175391"
        },
        {
            "topic_name": "Penipuan",
            "interest_score": 0.0001664813,
            "rank": 1,
            "topic_id": "347858007"
        },
        {
            "topic_name": "Perbankan",
            "interest_score": 0.0004181841,
            "rank": 1,
            "topic_id": "33160611"
        },
        {
            "topic_name": "Hollywood",
            "interest_score": 0.0004338053,
            "rank": 1,
            "topic_id": "47094765"
        },
        {
            "topic_name": "Internet",
            "interest_score": 0.0004971035,
            "rank": 1,
            "topic_id": "82233981"
        },
        {
            "topic_name": "Infrastruktur",
            "interest_score": 0.0006843708000000001,
            "rank": 1,
            "topic_id": "38682224"
        },
        {
            "topic_name": "Smartphone",
            "interest_score": 0.000692263,
            "rank": 1,
            "topic_id": "41661376"
        },
        {
            "topic_name": "Survei",
            "interest_score": 0.0016380414000000002,
            "rank": 1,
            "topic_id": "46859735"
        },
        {
            "topic_name": "Horor",
            "interest_score": 0.0073969624,
            "rank": 1,
            "topic_id": "338820867"
        },
        {
            "topic_name": "Parenting",
            "interest_score": 0.009060816400000001,
            "rank": 1,
            "topic_id": "40710353"
        },
        {
            "topic_name": "Kuliner",
            "interest_score": 0.0101298267,
            "rank": 1,
            "topic_id": "22661216"
        },
        {
            "topic_name": "Keluarga",
            "interest_score": 0.022234603000000002,
            "rank": 1,
            "topic_id": "27312625"
        },
        {
            "topic_name": "Otomotif",
            "interest_score": 0.030306936200000002,
            "rank": 1,
            "topic_id": "22552435"
        },
        {
            "topic_name": "Media Sosial",
            "interest_score": 0.0867776779,
            "rank": 1,
            "topic_id": "27313197"
        },
        {
            "topic_name": "Sains",
            "interest_score": 0.1698616142,
            "rank": 1,
            "topic_id": "27433171"
        },
        {
            "topic_name": "Sejarah",
            "interest_score": 0.17942875260000002,
            "rank": 1,
            "topic_id": "27428824"
        },
        {
            "topic_name": "Regional",
            "interest_score": 0.3892814956,
            "rank": 1,
            "topic_id": "33020303"
        },
        {
            "topic_name": "Sepak Bola",
            "interest_score": 0.39614503030000003,
            "rank": 1,
            "topic_id": "27432949"
        },
        {
            "topic_name": "Pembunuhan",
            "interest_score": 0.4050528284,
            "rank": 1,
            "topic_id": "39328335"
        },
        {
            "topic_name": "Sports",
            "interest_score": 0.4601020186,
            "rank": 1,
            "topic_id": "40723504"
        },
        {
            "topic_name": "Bisnis",
            "interest_score": 0.5393272529,
            "rank": 1,
            "topic_id": "22552186"
        },
        {
            "topic_name": "Liputan Khusus",
            "interest_score": 1.3833275839999999,
            "rank": 1,
            "topic_id": "1065124711"
        },
        {
            "topic_name": "Nasional",
            "interest_score": 1.5661961556000001,
            "rank": 1,
            "topic_id": "22552349"
        },
        {
            "topic_name": "News",
            "interest_score": 375.8533696987,
            "rank": 1,
            "topic_id": "22553543"
        },
        {
            "topic_name": "Tips Percintaan",
            "interest_score": 1.558e-7,
            "rank": 1,
            "topic_id": "27431110790312447"
        },
        {
            "topic_name": "Film",
            "interest_score": 0.0000130029,
            "rank": 1,
            "topic_id": "24575829"
        },
        {
            "topic_name": "Ibu Menyusui",
            "interest_score": 0.000010771200000000001,
            "rank": 1,
            "topic_id": "774386537"
        },
        {
            "topic_name": "Narkotika",
            "interest_score": 0.0000104756,
            "rank": 1,
            "topic_id": "43159641"
        }
    ]

import time
start_total_time = time.time()
n = list(c)
end_total_time = time.time() - start_total_time
print 'Time taken to transform output: %.7f' % end_total_time

import pandas as pd
df = pd.DataFrame(d)
df_mod = df[['topic_id', 'topic_name', 'interest_score']]
df_mod['rank'] = df_mod['interest_score'].rank(ascending=False)
print df_mod