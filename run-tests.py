
import os

PROJECT_DIR = os.path.abspath(os.path.dirname( __file__ ))
SRC_DIR = os.path.join(PROJECT_DIR, "src")
TEST_DIR = os.path.join(PROJECT_DIR, "test")

def runtestdir(subdir):
    #cwd = os.getcwd()
    #subdir = os.path.join(cwd, subdir)
    entries = os.listdir(subdir)
    total = 0
    errs = 0
    for f in entries:
        if not f.endswith(".py"):
            continue
        if not f.startswith("test_"):
            continue
        cmd = "python %s/%s" % (subdir, f)
        print "FILE: %s/%s" % (subdir, f)
        exit_code = os.system(cmd)
        total += 1
        if exit_code != 0:
            errs += 1
    print "SUMMARY: %s -> %s total / %s error" % (subdir, total, errs)


if __name__ == "__main__":
    #
    os.chdir(TEST_DIR)
    #
    os.environ["PYTHONPATH"] = ":".join([SRC_DIR, TEST_DIR])
    #
    runtestdir("bindertest")

