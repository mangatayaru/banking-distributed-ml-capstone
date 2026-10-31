import argparse, json, random
from datetime import datetime, timezone
from pathlib import Path

p=argparse.ArgumentParser()
p.add_argument("--output",default="streaming/incoming")
p.add_argument("--count",type=int,default=1000)
p.add_argument("--batch-size",type=int,default=100)
args=p.parse_args()
random.seed(17)
out=Path(args.output); out.mkdir(parents=True,exist_ok=True)
accounts=[f"ACCT{i:05d}" for i in range(1,201)]
for start in range(0,args.count,args.batch_size):
    events=[]
    for i in range(start,min(start+args.batch_size,args.count)):
        amount=round(random.expovariate(1/120),2)
        if i%97==0: amount=round(random.uniform(2500,7000),2)
        events.append({"transaction_id":f"TXN{i+1:07d}","account_id":random.choice(accounts),
        "event_time":datetime.now(timezone.utc).isoformat(),"amount":amount,
        "merchant_category":random.choice(["grocery","fuel","travel","online","utilities"]),
        "channel":random.choice(["card","upi","netbanking"]),
        "country":random.choice(["IN","IN","IN","SG","GB"])})
    path=out/f"batch_{start//args.batch_size:04d}.json"
    path.write_text("\\n".join(json.dumps(e) for e in events)+"\\n",encoding="utf-8")
    print(f"Wrote {len(events)} events -> {path}")
