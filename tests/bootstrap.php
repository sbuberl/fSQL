<?php

require 'vendor/autoload.php';

$cacheFunction = null;

if (extension_loaded('Zend OpCache') && ini_get('opcache.enable_cli')) {
    if (function_exists('opcache_compile_file')) {
        $cacheFunction = 'opcache_compile_file';
    }
} elseif (extension_loaded('apc') && ini_get('apc.enable_cli')) {
    $cacheFunction = 'apc_compile_file';
}

if ($cacheFunction !== null) {
    $directory = dirname(__DIR__).'/src';
    $files = new FilesystemIterator($directory);
    foreach ($files as $entry) {
        if ($entry->getExtension() === 'php') {
            $cacheFunction($entry->getPathname());
        }
    }
}
