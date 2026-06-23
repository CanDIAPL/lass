import re
import matplotlib.pyplot as plt

log_pattern = (
    r'(\d{4}-\d{2}-\d{2})\s(\d{2}:\d{2}:\d{2}).+Handling.'
    r'(\d+).+(?:\n.+){9}\n.+:\s(\d\.\d+)\n.+:\s(\d\.\d+)\n.+:\s(\d\.\d+)'
)

tstamps, parse_ts, wrangle_ts, store_ts = [], [], [] ,[]
alertcount = 0
with open('brokerpoll_counts_LASS.log', 'r+') as f:
    text = f.read()
    match = re.findall(log_pattern, text)
    if match:
        # print("found", match.groups())
        # [print(m) for m in match]
        for m in match:
            d, t, a, pt, wt, st = m
            # print(d, t, a, pt, wt, st)
            tstamps.append((d,t))
            alertcount += int(a)
            parse_ts.append(float(pt))
            wrangle_ts.append(float(wt))
            store_ts.append(float(st))

print(parse_ts)
plt.plot(parse_ts, label='parse time')
plt.plot(wrangle_ts, label='wrangle time')
plt.plot(store_ts, label='store time')
plt.xlabel("alert block (10 alerts each)")
plt.ylabel("Time (s)")
plt.legend()
plt.savefig('timseries.pdf')
plt.show()

# if __name__ == "__main__":
#     pass
