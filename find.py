import sys
import os
import argparse
from collections import defaultdict
from subprocess import Popen
from threading import Thread
from datetime import datetime
from time import sleep

FILE_LIST_DISPLAY_MAX_SIZE = 50

class DeleteThread(Thread):
    """background deletion thread"""
    def __init__(self, files_list, deletion_log_path):
        super(DeleteThread, self).__init__()
        self.files_list = files_list
        self.deletion_log_path = deletion_log_path

    def run(self):
        # Only importing here so that the rest of the features can be used
        # without this module, which requires extra installation and is also
        # only made for Windows.
        from send2trash import send2trash
        with open(self.deletion_log_path, 'a+') as deletion_log_file:
            for filepath in self.files_list:
                try:
                    send2trash(filepath)
                    deletion_log_file.write("%s: Deleted %s\n" % (datetime.now(), filepath))
                except Exception as e:
                    deletion_log_file.write("%s: Error while trying to delete %s: %s\n" % (datetime.now(), filepath, e))


def parse_args(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('--folders', type=str, help='folders to find duplicates in', nargs='+', required=True)
    parser.add_argument('--exclude-folders', type=str, help='folders to ignore', nargs='+', default=[])
    parser.add_argument('--exclude-filenames', type=str, help='file names (not paths) to ignore (NOT case-sensitive)', nargs='+', default=[])
    parser.add_argument('--exclude-folder-names', type=str, help='folder names (not paths) to ignore (NOT case-sensitive)', nargs='+', default=[])
    parser.add_argument('--extensions', type=str, default=[], help='if specified, will restrict the analysis to files ending with this extension (WITHOUT THE DOT) (NOT case-sensitive)', nargs='+')
    parser.add_argument('--output-path', '-o', type=str, help='path to write output to')
    parser.add_argument('--deletion-log-path', type=str, help='path to write deletion output log to', default='deletion_log.log')
    parser.add_argument('--interactive-delete', action='store_true', default=False)
    parser.add_argument('--check-size', action='store_true', default=False)
    parser.add_argument('--minimum-size', type=int, default=0, help='Minimum size (in bytes) for a file to be analyzed. Files smaller than this size will be ignored.')

    return parser.parse_args(argv)


def get_all_files_in_dir(options, dirname, global_dict, extension_filters):
    if not extension_filters:
        extension_filters = None
    else:
        extension_filters = set([e.lower() for e in extension_filters])

    for root, dirs, files in os.walk(dirname):
        skip = False
        folder_name = os.path.basename(os.path.normpath(root))
        if folder_name.lower() in options.exclude_folder_names:
            continue
        for exclude_folder in options.exclude_folders:
            if root.startswith(exclude_folder):
                skip = True
                break
        if skip:
            continue
        for file in files:
            if file.lower() in options.exclude_filenames:
                continue
            if extension_filters is not None:
                ext_pos = file.rfind('.')
                ext = file[ext_pos + 1:].lower()
                if ext not in extension_filters:
                    # Extension filters were specified and the file does not
                    # respect the filters, skipping it
                    continue

            if options.check_size or options.minimum_size > 0:
                try:
                    fpath = os.path.join(root, file)
                    statinfo = os.stat(fpath)
                    size = statinfo.st_size
                    if size < options.minimum_size:  # note: default option value is zero
                        continue  # skip this file
                except Exception as e:
                    print("Could not gather information about %s (%s), skipping." % (fpath, e))
                    continue

            if options.check_size:
                global_dict[file.lower()][size].append(root.lower())
            else:
                global_dict[file.lower()].append(root.lower())


def main(argv):
    options = parse_args(argv)
    print(options)
    try:
        global_dict = defaultdict(lambda: list())
        if options.check_size:
            global_dict = defaultdict(lambda: defaultdict(lambda: list()))
        for dirname in options.folders:
            print("Reading content of target folder \"%s\"..." % dirname)
            get_all_files_in_dir(options, dirname, global_dict, options.extensions)
            print("Done")
        print("Executing analysis...")
        dirpaths_with_dupes_counts, dirpaths_to_paths_with_common_files, filtered_files_dupes = analyse_gathered_files_info(options, global_dict)
        print("Done")
        print("Saving to output file...")
        write_to_output(options, options.output_path, filtered_files_dupes, dirpaths_with_dupes_counts, dirpaths_to_paths_with_common_files)
        print("Done")
        if options.interactive_delete:
            print("Starting interactive delete.")
            interactive_delete(options, filtered_files_dupes, dirpaths_with_dupes_counts, dirpaths_to_paths_with_common_files)
    except BaseException as e:
        print("Unexpected error happened.")
        print(e)

    print("Press enter to exit the program.")
    raw_input()


def interactive_delete(options, filtered_files_dupes, dirpaths_with_dupes_counts, dirpaths_to_paths_with_common_files):
    deletion_threads = []
    pairs_of_folders_passed = set()
    try:
        for dirpath, count in sorted(dirpaths_with_dupes_counts.items(), reverse=True, key=lambda x: x[1]):
            for dirpath_with_files_in_common, common_files in sorted(dirpaths_to_paths_with_common_files[dirpath].items(), reverse=True, key=lambda x: len(x[1])):
                curr_folders_tuple = (dirpath, dirpath_with_files_in_common)
                inverse_folders_tuple = (curr_folders_tuple[1], curr_folders_tuple[0])
                if inverse_folders_tuple in pairs_of_folders_passed:
                    print("%s and %s already passed, skipping..." % curr_folders_tuple)
                    continue
                common_files_count = len(common_files)
                pairs_of_folders_passed.add(curr_folders_tuple)
                files_list_disp = ""
                common_files_for_display = common_files[:FILE_LIST_DISPLAY_MAX_SIZE]

                filesinfo = []
                for _fname in common_files_for_display:
                    try:
                        fdir1_stat = os.stat(os.path.join(dirpath, _fname))
                        fdir1_size = fdir1_stat.st_size/1e6
                        fdir1_ctime = datetime.fromtimestamp(fdir1_stat.st_ctime)
                    except Exception as e:
                        fdir1_size = 0                        
                        fdir1_ctime = "ERROR reading file"                        

                    try:
                        fdir2_stat = os.stat(os.path.join(dirpath_with_files_in_common, _fname))
                        fdir2_size = fdir2_stat.st_size/1e6
                        fdir2_ctime = datetime.fromtimestamp(fdir2_stat.st_ctime)
                    except Exception as e:
                        fdir2_size = 0                        
                        fdir2_ctime = "ERROR reading file"

                    filesinfo.append("%s\t(0: %.3fmb\t%s\t1: %.3fmb\t%s)" % (
                        _fname,
                        fdir1_size,
                        fdir1_ctime,
                        fdir2_size,
                        fdir2_ctime,
                    ))

                files_list_disp = "\n%s\n" % "\n".join(filesinfo)
                if len(common_files) > FILE_LIST_DISPLAY_MAX_SIZE:
                    files_list_disp += "... [+ %s more files]\n" % (len(common_files) - FILE_LIST_DISPLAY_MAX_SIZE)
                open_explorer = ask_yesno(
                    "0: %s\n1: %s\nhave %s files in common. %s View them in explorer?" %
                    (dirpath, dirpath_with_files_in_common, common_files_count, files_list_disp),
                    default_yes=False,
                )
                if open_explorer:
                    try:
                        Popen('explorer %s' % dirpath)
                        # Delaying the second window by a second so that it does not go to background
                        sleep(1.5)
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
                files_deletion_list = [os.path.join(dirpath if folder == '0' else dirpath_with_files_in_common, filename) for filename in common_files]
                del_thread = DeleteThread(files_deletion_list, options.deletion_log_path)
                deletion_threads.append(del_thread)
                del_thread.start()
                print("Deleting %s files in background..." % len(files_deletion_list))
    except BaseException:
        raise
    finally:
        print("Waiting for background deletion threads to finish...")
        for del_thread in deletion_threads:
            del_thread.join(600)
        print("Done")



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
        elif res == '':
            res = 'y' if default_yes else 'n'
    sys.stdout.write("\n")
    return res == 'y'


def analyse_gathered_files_info(options, global_dict):
    dirpaths_with_dupes_counts = defaultdict(lambda: 0)
    dirpaths_to_paths_with_common_files = defaultdict(lambda: defaultdict(lambda: []))
    filtered_files_dupes = defaultdict(lambda: defaultdict(lambda: []))
    for file, paths in global_dict.iteritems():
        if options.check_size:
            size_to_dirpaths = paths
            for size, paths in size_to_dirpaths.items():            
                if len(paths) > 1:  # only write files that actually have duplicates...
                    for dirpath in paths:
                        dirpaths_with_dupes_counts[dirpath] += 1
                        for dirpath2 in paths:
                            if dirpath == dirpath2:
                                continue
                            dirpaths_to_paths_with_common_files[dirpath][dirpath2].append(file)
                        filtered_files_dupes[file][size].append(dirpath)
        else:
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


def write_to_output(options, outpath, filtered_files_dupes, dirpaths_with_dupes_counts, dirpaths_to_paths_with_common_files):
    if not outpath:
        outpath = "./file_duplicates.txt"
 
    with open(outpath, "w+", buffering=int(50e3)) as fout:
        if not options.check_size:
            for file, dirpaths in filtered_files_dupes.items():
                line = "%s\t%s\n" % (file, "\t".join(dirpaths))
                fout.write(line)
        else:
            for file, size_to_dirpaths in filtered_files_dupes.items():
                for size, dirpaths in size_to_dirpaths.items():
                    line = "%s\t%s\t%s\n" % (file, size, "\t".join(dirpaths))
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