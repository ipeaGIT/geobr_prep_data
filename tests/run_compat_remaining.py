"""Run CompatMalhas for the 10 remaining UFs that previously failed."""
import subprocess
import sys
import os
import time

python = sys.executable
script = os.path.join(os.path.dirname(__file__), '..', 'scripts', 'compat_2000_2010.py')
compat_dir = os.path.join(os.path.dirname(__file__), '..', 'data-raw', 'census_tract', 'compat')

# 10 UFs that still need processing (25 and 35 already done)
ufs = ['15', '22', '23', '26', '29', '31', '33', '41', '43', '51']

results = {}
for uf in ufs:
    # Delete cached files to force re-run
    for suffix in [f'_C2000_{uf}.csv', f'_C2010_{uf}.csv', '_MCA.gpkg', '_report.json']:
        f = os.path.join(compat_dir, f'compat_{uf}{suffix}')
        if os.path.exists(f):
            os.remove(f)

    print(f"\n{'='*60}")
    print(f"Running UF {uf}...")
    print(f"{'='*60}")
    t0 = time.time()
    try:
        result = subprocess.run(
            [python, script, uf],
            capture_output=True, text=True, timeout=1800  # 30 min timeout
        )
        elapsed = time.time() - t0
        if result.returncode == 0:
            csv_path = os.path.join(compat_dir, f'compat_{uf}_C2000_{uf}.csv')
            if os.path.exists(csv_path):
                results[uf] = f"OK ({elapsed:.0f}s)"
                print(result.stdout[-500:] if len(result.stdout) > 500 else result.stdout)
            else:
                results[uf] = f"FAIL: no CSV produced ({elapsed:.0f}s)"
        else:
            results[uf] = f"FAIL: exit code {result.returncode} ({elapsed:.0f}s)"
            print(result.stderr[-500:] if result.stderr else "no stderr")
    except subprocess.TimeoutExpired:
        elapsed = time.time() - t0
        results[uf] = f"TIMEOUT ({elapsed:.0f}s)"
    except Exception as e:
        elapsed = time.time() - t0
        results[uf] = f"ERROR: {e} ({elapsed:.0f}s)"

print(f"\n{'='*60}")
print("SUMMARY")
print(f"{'='*60}")
for uf, status in results.items():
    siglas = {'15':'PA','22':'PI','23':'CE','26':'PE','29':'BA',
              '31':'MG','33':'RJ','41':'PR','43':'RS','51':'MT'}
    print(f"  UF {uf} ({siglas[uf]}): {status}")

n_ok = sum(1 for v in results.values() if v.startswith("OK"))
print(f"\nTotal: {n_ok}/{len(ufs)} succeeded")
