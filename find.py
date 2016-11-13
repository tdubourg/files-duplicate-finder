import sys
import os
import argparse
from collections import defaultdict
from subprocess import Popen

def parse_args(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('--folders', type=str, help='folders to find duplicates in', nargs='+')
    parser.add_argument('--extensions', type=str, default=[], help='if specified, will restrict the analysis to files ending with this extension (WITHOUT THE DOT) (NOT case-sensitive)', nargs='+')
    parser.add_argument('--output-path', '-o', type=str, help='path to write output to')
    parser.add_argument('--interactive-delete', action='store_true', default=False)

    return parser.parse_args(argv)


def get_all_files_in_dir(dirname, global_dict, extension_filters):
    if not extension_filters:
        extension_filters = None
    else:
        extension_filters = set([e.lower() for e in extension_filters])

    for root, dirs, files in os.walk(dirname):
        for file in files:
            if extension_filters is not None:
                ext_pos = file.rfind('.')
                ext = file[ext_pos + 1:]
                if ext not in extension_filters:
                    # Extension filters were specified and the file does not
                    # respect the filters, skipping it
                    continue
            global_dict[file.lower()].append(root.lower())
            # fpath = os.path.join(root, file)
            # fpaths.append(fpath)


def main(argv):
    options = parse_args(argv)
    global_dict = defaultdict(lambda: list())
    for dirname in options.folders:
        get_all_files_in_dir(dirname, global_dict, options.extensions)
    dirpaths_with_dupes_counts, dirpaths_to_paths_with_common_files, filtered_files_dupes = analyse_gathered_files_info(global_dict)
    write_to_output(options.output_path, filtered_files_dupes, dirpaths_with_dupes_counts, dirpaths_to_paths_with_common_files)
    if options.interactive_delete:
        interactive_delete(filtered_files_dupes, dirpaths_with_dupes_counts, dirpaths_to_paths_with_common_files)


def interactive_delete(filtered_files_dupes, dirpaths_with_dupes_counts, dirpaths_to_paths_with_common_files):
    # Only importing here so that the rest of the features can be used without this module, which requires extra
    # installation and is also only made for Windows.
    from send2trash import send2trash
    for dirpath, count in sorted(dirpaths_with_dupes_counts.items(), reverse=True, key=lambda x: x[1]):
        for dirpath_with_files_in_common, common_files in sorted(dirpaths_to_paths_with_common_files[dirpath].items(), reverse=True, key=lambda x: len(x[1])):
            common_files_count = len(common_files)
            skip = not ask_yesno(
                "%s and %s have %s files in common. View them?" %
                (dirpath, dirpath_with_files_in_common, common_files_count),
                default_yes=True,
            )
            if skip:
                continue
            try:
                Popen('explorer %s' % dirpath)
                Popen('explorer %s' % dirpath_with_files_in_common)
            except Exception as e:
                print("Error:", e)
                print("Skipping...")
                continue
            remove = ask_yesno("Remove files from one of the folders?", default_yes=False)
            if not remove:
                continue
            print("Which folder? [0/1/Abort]")
            print("0: %s\n1: %s" % (dirpath, dirpath_with_files_in_common))
            folder = raw_input()
            if folder not in ('0', '1'):
                # abort
                print("Skipping")
                continue
            for filename in common_files:
                filepath = os.path.join(dirpath if folder == '0' else dirpath_with_files_in_common, filename)
                try:
                    send2trash(filepath)
                    print ("Deleted %s" % filepath)
                except Exception as e:
                    print("Error while trying to delete %s: %s" % (filepath, e))


def ask_yesno(msg, default_yes=False):
    """
        asks the user yes/no, returns True for yes, False for no
    """
    res = None
    while res not in ('y', 'n'):
        sys.stdout.write("\n%s [%s/%s] " % (msg, 'Y' if default_yes else 'y', 'n' if default_yes else 'N'))
        res = raw_input().lower()
        if res == 'yes':
            res = 'y'
        elif res == 'no':
            res = 'n'
    sys.stdout.write("\n")
    return res == 'y'


def analyse_gathered_files_info(global_dict):
    dirpaths_with_dupes_counts = defaultdict(lambda: 0)
    dirpaths_to_paths_with_common_files = defaultdict(lambda: defaultdict(lambda: []))
    filtered_files_dupes = {}
    for file, paths in global_dict.iteritems():
        if len(paths) > 1:  # only write files that actually have duplicates...
            filtered_files_dupes[file] = []
            for dirpath in paths:
                dirpaths_with_dupes_counts[dirpath] += 1
                for dirpath2 in paths:
                    if dirpath == dirpath2:
                        continue
                    dirpaths_to_paths_with_common_files[dirpath][dirpath2].append(file)
                filtered_files_dupes[file].append(dirpath)
    return dirpaths_with_dupes_counts, dirpaths_to_paths_with_common_files, filtered_files_dupes


def write_to_output(outpath, filtered_files_dupes, dirpaths_with_dupes_counts, dirpaths_to_paths_with_common_files):
    if not outpath:
        outpath = "./file_duplicates.txt"
 
    with open(outpath, "w+", buffering=int(50e3)) as fout:
        for file, dirpaths in filtered_files_dupes.items():
            line = "%s\t%s\n" % (file, "\t".join(dirpaths))
            fout.write(line)
        # Duplicates summary per folder:
        fout.write("#" * 200)
        fout.write("\n")
        for dirpath, count in sorted(dirpaths_with_dupes_counts.items(), reverse=True, key=lambda x: x[1]):
            fout.write("%s\t%s\n" % (dirpath, count))
            for dirpath_with_files_in_common, common_files in sorted(dirpaths_to_paths_with_common_files[dirpath].items(), reverse=True, key=lambda x: len(x[1])):
                fout.write("\t%s\t%s\n" % (dirpath_with_files_in_common, len(common_files)))

if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))