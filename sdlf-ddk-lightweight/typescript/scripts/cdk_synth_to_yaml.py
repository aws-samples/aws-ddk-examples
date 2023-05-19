import codecs
import os
import subprocess
import sys

cicd_enabled = sys.argv[1]

os.chdir("../")

cmd = ["rm", "-rf", "yaml"]
subprocess.run(cmd)
cmd = ["mkdir", "yaml"]
subprocess.run(cmd)


if cicd_enabled == "true":
    print("CICD is enabled")
    f = open(f"yaml/sdlf-ddk-cicd.yaml", "w")
    cmd = ["cdk", "synth"]
    subprocess.run(cmd, stdout=f)
    print(f"Creating YAML for sdlf-ddk-cicd")
else:
    print("CICD is not enabled")
    ls_lines = subprocess.run(
        ["cdk", "list"], stdout=subprocess.PIPE
    ).stdout.splitlines()
    ls_lines_decode = list(map(lambda n: codecs.decode(n), ls_lines))

    for lines in ls_lines_decode:
        print(f"Creating YAML for {lines}")
        f = open(f'yaml/{lines.replace("/","-")}.yaml', "w")
        cmd = ["cdk", "synth", lines]
        subprocess.run(cmd, stdout=f)
