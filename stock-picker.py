import sys
import os

summary = os.environ["GITHUB_STEP_SUMMARY"]

with open(summary, "a") as f:
    f.write("## Script Results\n")
    f.write("- Users: 42\n")
    f.write("- Status: OK\n")

print("Argument:", sys.argv[1])
