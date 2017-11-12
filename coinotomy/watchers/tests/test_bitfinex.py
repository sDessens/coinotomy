import unittest
import logging

from coinotomy.backend.ramdiskbackend import RamdiskStorageBackend
from coinotomy.watchers.bitfinex import WatcherBitfinex, BitfinexAPI

log = logging.getLogger("dummy")


def is_sorted(trades):
    return all(a[0] <= b[0] for a, b in zip(trades[:-1], trades[1:]))


class TestWatcherBtce(unittest.TestCase):

    # an actual response of 120 trades
    RESPONSE_1 = '[[4145,1358182043000,0.2721858,14.5373664],[4149,1358185856000,20,14.5329498],[4161,1358217143000,44,14.5247898],[4163,1358228710000,20,14.7685698],[4173,1358336761000,0.2,14.7492],[4183,1358341386000,0.1,14.779596],[4193,1358343991000,1,14.79],[4197,1358344069000,10,14.790306],[4199,1358344073000,10,14.790306],[4201,1358344159000,1,14.8001898],[4203,1358344162000,1,14.8001898],[4207,1358347451000,20,14.79],[4211,1358355113000,0.14426,14.8512],[4213,1358356677000,0.65108446,14.8613898],[4221,1358367564000,0.39442763,14.9634],[4223,1358367586000,0.004,14.96952],[4225,1358367587000,0.00026904,14.96952],[4227,1358367589000,0.0001631,14.96952],[4229,1358367591000,0.03088904,14.96952],[4231,1358367593000,0.00025188,14.96952],[4233,1358367594000,0.00029679,14.96952],[4235,1358367596000,0.00029044,14.96952],[4237,1358367598000,33.36468455,14.96952],[4239,1358367625000,0.03833329,14.9633898],[4241,1358367629000,4.54956329,14.9633898],[4243,1358367665000,1,14.96238],[4245,1358367681000,4.41246,14.9633898],[4247,1358367731000,1,14.96238],[4249,1358368725000,60,14.9827494],[4251,1358368829000,3,14.9827698],[4253,1358368919000,0.121,14.98278],[4255,1358373186000,9,15.0246],[4267,1358406173000,0.09191701,15.1367898],[4269,1358406304000,0.09191701,15.1367898],[4271,1358407635000,0.09080779,15.1367898],[4273,1358407636000,0.2,15.1367898],[4275,1358407636000,0.14907964,15.1367898],[4277,1358407642000,1.3,15.1367898],[4279,1358408747000,0.5,15.1571898],[4281,1358415914000,50,15.1775694],[4283,1358416467000,7,15.1775898],[4285,1358416504000,0.1,15.1775796],[4291,1358418191000,25,15.1775898],[4293,1358425644000,0.97986214,15.2083938],[4299,1358427849000,0.001,15.147],[4321,1358432875000,191,15.289596],[4327,1358435042000,86.77,15.2897694],[4331,1358440502000,33,15.3110874],[4333,1358440511000,3,15.3407898],[4335,1358440578000,0.11516152,15.3445536],[4337,1358442804000,6.143,15.555],[4351,1358448684000,0.3,15.8628666],[4353,1358448825000,0.02016495,15.861],[4355,1358449377000,0.0601495,15.93138],[4365,1358451967000,0.10802333,15.861],[4367,1358453515000,54.5264,15.8099796],[4369,1358456921000,0.06539399,15.7769316],[4371,1358456997000,0.09709331,15.8061852],[4373,1358457119000,25.42967326,15.8285232],[4375,1358457231000,6.05249,15.7751364],[4377,1358457383000,0.82659079,15.8536662],[4379,1358457464000,0.4,15.8599596],[4381,1358457533000,0.03862403,15.759],[4383,1358458294000,0.91644183,15.81],[4385,1358458739000,0.02,15.7793898],[4387,1358458789000,0.01669038,15.8608674],[4389,1358459744000,3.38182984,15.861],[4391,1358460428000,0.2227,15.90384],[4393,1358461285000,0.06816193,15.9017898],[4395,1358461347000,1,15.90996],[4397,1358464825000,2.68970424,15.9119898],[4399,1358466291000,0.001,15.9628878],[4401,1358466507000,0.01752827,15.964683],[4403,1358467672000,0.02132559,16.0343694],[4409,1358471278000,4.75745094,16.1057898],[4413,1358486149000,0.2,16.218],[4417,1358489715000,1.7,16.2588],[4419,1358489837000,0.2372,16.262778],[4421,1358490170000,1,16.2486],[4423,1358491083000,0.07472682,16.25064],[4427,1358494138000,0.00000471,16.1965596],[4433,1358502600000,0.85,15.81],[4435,1358504054000,0.01,15.7488],[4437,1358504131000,6.2,15.759],[4439,1358505169000,24.99,15.7488],[4441,1358508916000,0.13786702,15.962949],[4443,1358508920000,0.0004,15.962949],[4445,1358508925000,0.2399684,15.962949],[4449,1358509947000,11.9,15.8864286],[4451,1358510115000,12.11615186,15.886398],[4453,1358510122000,0.98384814,15.8863776],[4455,1358510244000,0.03472378,15.9619494],[4459,1358511476000,0.1,15.9119898],[4461,1358511684000,0.00839244,15.8145288],[4463,1358511685000,25,15.8145288],[4465,1358512484000,15,15.9222],[4469,1358513760000,25,15.963],[4471,1358516179000,25,16.0242],[4475,1358519212000,17.7487871,15.9629898],[4479,1358529406000,1,16.014],[4481,1358529807000,1,16.0343694],[4487,1358534814000,4.94961867,16.15986],[4489,1358535167000,3.10195461,16.1567694],[4491,1358535495000,1,16.1557698],[4493,1358535667000,0.87,16.0242],[4495,1358535763000,1,16.0242],[4497,1358536130000,1,16.014],[4499,1358538751000,0.0360577,16.0608894],[4501,1358538752000,0.0197147,16.0608894],[4505,1358540598000,3,15.9931818],[4507,1358542958000,2.86,15.9017898],[4511,1358544009000,0.026368,15.8405898],[4513,1358544009000,0.00130138,15.8405898],[4515,1358544010000,15,15.8405898],[4517,1358544663000,3,15.9629898],[4519,1358544868000,20,15.9426],[4521,1358546742000,23.42804603,16.0841658],[4523,1358546761000,6.04476619,16.084176],[4525,1358547716000,9.71246064,15.9923556],[4527,1358548082000,4,15.9119898]]'

    # an actual response of 3 trades
    RESPONSE_2 = '[[4145,1358182043000,0.2721858,14.5373664],[4149,1358185856000,20,14.5329498],[4161,1358217143000,44,14.5247898]]'
    RESPONSE_2_EXPECTED = [(1358182043.0, 14.5373664, 0.2721858), (1358185856.0, 14.5329498, 20.0), (1358217143.0, 14.5247898, 44.0)]


    def setUp(self):
        self.backend = RamdiskStorageBackend()
        self.watcher = WatcherBitfinex("btc/usd", "tBTCUSD")
        self.api = BitfinexAPI("tBTCUSD", log)

    def tearDown(self):
        del self.api

    def test_network(self):
        trades, newest_ts, newest_tid = self.api.more()
        self.assertEqual(len(trades), 120)
        self.assertIsInstance(newest_ts, float)

    def test_parse_large_response(self):
        trades, newest_ts, newest_tid = self.api._parse_response(self.RESPONSE_1, 0, 4161)
        self.assertEqual(newest_ts, 1358548082.0)  # Friday, January 18, 2013 10:28:02 PM
        self.assertEqual(newest_tid, 4527)
        for row in trades:
            self.assertIsInstance(row[0], float)
            self.assertIsInstance(row[1], float)
            self.assertIsInstance(row[2], float)
        self.assertTrue(is_sorted(trades))
        self.assertTrue(len(trades), 120-3)

    def test_parse_small_response(self):
        trades, newest_ts, newest_tid = self.api._parse_response(self.RESPONSE_2)
        self.assertEqual(newest_ts, 1358217143)
        self.assertEqual(newest_tid, 4161)
        self.assertEqual(trades, self.RESPONSE_2_EXPECTED)
