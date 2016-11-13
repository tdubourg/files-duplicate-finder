import sys
import os
import argparse
from collections import defaultdict

def parse_args(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('--folders', type=str, help='folders to find duplicates in', nargs='+')
    parser.add_argument('--extensions', type=str, default=[], help='if specified, will restrict the analysis to files ending with this extension (WITHOUT THE DOT) (NOT case-sensitive)', nargs='+')
    parser.add_argument('--output-path', '-o', type=str, help='path to write output to')

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
    write_to_output(options.output_path, global_dict)
    # print(global_dict.items())


def write_to_output(outpath, global_dict):
    if not outpath:
        outpath = "./file_duplicates.txt"
 
    dirpaths_with_dupes_counts = defaultdict(lambda: 0)
    dirpaths_to_paths_with_common_files = defaultdict(lambda: defaultdict(lambda: 0))
    with open(outpath, "w+", buffering=int(50e3)) as fout:
        for file, paths in global_dict.iteritems():
            if len(paths) > 1:  # only write files that actually have duplicates...
                line = file
                for dirpath in paths:
                    dirpaths_with_dupes_counts[dirpath] += 1
                    for dirpath2 in paths:
                        if dirpath == dirpath2:
                            continue
                        dirpaths_to_paths_with_common_files[dirpath][dirpath2] += 1
                    line += "\t%s" % dirpath
                fout.write(line + "\n")
        # Duplicates summary per folder:
        fout.write("#" * 200)
        fout.write("\n")
        for dirpath, count in sorted(dirpaths_with_dupes_counts.items(), reverse=True, key=lambda x: x[1]):
            fout.write("%s\t%s\n" % (dirpath, count))
            for dirpath_with_files_in_common, common_files_count in sorted(dirpaths_to_paths_with_common_files[dirpath].items(), reverse=True, key=lambda x: x[1]):
                fout.write("\t%s\t%s\n" % (dirpath_with_files_in_common, common_files_count))

if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))