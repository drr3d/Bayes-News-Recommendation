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

import time
start_total_time = time.time()
n = list(c)
end_total_time = time.time() - start_total_time
print 'Time taken to transform output: %.7f' % end_total_time