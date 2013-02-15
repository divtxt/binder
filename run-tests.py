
import os, sys

PROJECT_DIR = os.path.abspath(os.path.dirname( __file__ ))

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
        print >> sys.stderr, "FILE:", test_file
        exit_code = os.system(sys.executable + " " + test_file)
        total += 1
        if exit_code != 0:
            errs += 1
    print >> sys.stderr, "SUMMARY: %s -> %s total / %s error (%s)" \
        % (subdir, total, errs, sys.executable)


if __name__ == "__main__":
    os.chdir(PROJECT_DIR)
    os.environ["PYTHONPATH"] = PROJECT_DIR
    runtestdir("bindertest")

