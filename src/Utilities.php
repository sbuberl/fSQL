<?php

namespace FSQL;

final class Utilities
{
    /**
     * @codeCoverageIgnore
     */
    private function __construct()
    {
    }

    public static function createDirectory($original_path, $type, Environment $environment)
    {
        $paths = pathinfo($original_path);

        $dirname = realpath($paths['dirname']);
        if (!$dirname || !is_dir($dirname) || !is_readable($dirname)) {
            if (@mkdir($original_path, 0777, true) === true) {
                $realpath = $original_path;
            } else {
                return $environment->set_error(ucfirst($type)." parent path '{$paths['dirname']}' does not exist.  Please correct the path or create the directory.");
            }
        }

        $path = $dirname.'/'.$paths['basename'];
        $realpath = realpath($path);
        if ($realpath === false || !file_exists($realpath)) {
            if (@mkdir($path, 0777, true) === true) {
                $realpath = $path;
            } else {
                return $environment->set_error("Unable to create directory '$path'.  Please make the directory manually or check the permissions of the parent directory.");
            }
        } elseif (!is_readable($realpath) || !is_writable($realpath)) {
            @chmod($realpath, 0777);
        }

        if (substr($realpath, -1) != '/') {
            $realpath .= '/';
        }

        if (is_dir($realpath) && is_readable($realpath) && is_writable($realpath)) {
            return $realpath;
        } else {
            return $environment->set_error("Path to directory for $type is not valid.  Please correct the path or create the directory and check that is readable and writable.");
        }
    }

    public static function deleteDirectory($dirPath)
    {
        if (is_dir($dirPath)) {
            if (substr($dirPath, strlen($dirPath) - 1, 1) != '/') {
                $dirPath .= '/';
            }
            $files = glob($dirPath.'*', GLOB_MARK);
            foreach ($files as $file) {
                if (is_dir($file)) {
                    self::deleteDirectory($file);
                } else {
                    unlink($file);
                }
            }

            rmdir($dirPath);

            return true;
        }

        return false;
    }

    public static function innerJoin($left_data, $right_data, $join_comparator)
    {
        if (empty($left_data) || empty($right_data)) {
            return [];
        }

        $new_join_data = [];

        foreach ($left_data as $left_entry) {
            foreach ($right_data as $right_entry) {
                if ($join_comparator($left_entry, $right_entry)) {
                    $new_join_data[] = array_merge($left_entry, $right_entry);
                }
            }
        }

        return $new_join_data;
    }

    public static function leftJoin($left_data, $right_data, $join_comparator, $pad_length, &$joinMatches)
    {
        $new_join_data = [];
        $right_padding = array_fill(0, $pad_length, null);

        foreach ($left_data as $left_row => $left_entry) {
            $match_found = false;
            foreach ($right_data as $right_row => $right_entry) {
                if ($join_comparator($left_entry, $right_entry)) {
                    $match_found = true;
                    $joinMatches[$left_row] = $right_row;
                    $new_join_data[] = array_merge($left_entry, $right_entry);
                }
            }

            if (!$match_found) {
                $new_join_data[] = array_merge($left_entry, $right_padding);
                $joinMatches[$left_row] = false;
            }
        }

        return $new_join_data;
    }

    public static function rightJoin($left_data, $right_data, $join_comparator, $pad_length)
    {
        $new_join_data = [];
        $left_padding = array_fill(0, $pad_length, null);

        foreach ($right_data as $right_entry) {
            $match_found = false;
            foreach ($left_data as $left_entry) {
                if ($join_comparator($left_entry, $right_entry)) {
                    $match_found = true;
                    $new_join_data[] = array_merge($left_entry, $right_entry);
                }
            }

            if (!$match_found) {
                $new_join_data[] = array_merge($left_padding, $right_entry);
            }
        }

        return $new_join_data;
    }

    public static function fullJoin($left_data, $right_data, $join_comparator, $left_pad_length, $right_pad_length)
    {
        $new_join_data = [];
        $matched_rids = [];
        $left_padding = array_fill(0, $left_pad_length, null);
        $right_padding = array_fill(0, $right_pad_length, null);

        foreach ($left_data as $left_entry) {
            $match_found = false;
            foreach ($right_data as $rid => $right_entry) {
                if ($join_comparator($left_entry, $right_entry)) {
                    $match_found = true;
                    $new_join_data[] = array_merge($left_entry, $right_entry);
                    if (!in_array($rid, $matched_rids)) {
                        $matched_rids[] = $rid;
                    }
                }
            }

            if (!$match_found) {
                $new_join_data[] = array_merge($left_entry, $right_padding);
            }
        }

        $unmatched_rids = array_diff(array_keys($right_data), $matched_rids);
        foreach ($unmatched_rids as $rid) {
            $new_join_data[] = array_merge($left_padding, $right_data[$rid]);
        }

        return $new_join_data;
    }
}
