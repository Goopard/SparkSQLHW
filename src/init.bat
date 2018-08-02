scp -P 2222 bids.py root@localhost:/usr/test.py
scp -P 2222 bid_classes.py root@localhost:/usr/bid_classes.py
ssh root@localhost -p 2222 command spark-submit test.py