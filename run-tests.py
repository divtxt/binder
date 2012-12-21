
import os, sys

PROJECT_DIR = os.path.abspath(os.path.dirname( __file__ ))
SRC_DIR = os.path.join(PROJECT_DIR, "src")
TEST_DIR = os.path.join(PROJECT_DIR, "test")

def runtestdir(subdir):
    entries = os.listdir(subdir)
    total = 0
    errs = 0
    for f in entries:
        if not f.endswith(".py"):
            continue
        if not f.startswith("test_"):
            continue
        test_file = os.path.join(subdir, f)
        print "FILE:", test_file
        exit_code = os.system(sys.executable + " " + test_file)
        total += 1
        if exit_code != 0:
            errs += 1
    print "SUMMARY: %s -> %s total / %s error (%s)" \
        % (subdir, total, errs, sys.executable)


if __name__ == "__main__":
    os.chdir(TEST_DIR)
    os.environ["PYTHONPATH"] = ":".join([SRC_DIR, TEST_DIR])
    runtestdir("bindertest")

