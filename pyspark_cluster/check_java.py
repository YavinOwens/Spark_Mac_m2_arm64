import os
import subprocess

print("JAVA_HOME:", os.environ.get("JAVA_HOME"))
print("\njava -version output:")
print(subprocess.check_output("java -version", shell=True, stderr=subprocess.STDOUT).decode()) 