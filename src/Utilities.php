<?php

namespace FSQL;

final class Utilities
{
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
}
