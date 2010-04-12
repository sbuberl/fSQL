<?php

/*
 * File only included if running PHP 5.
 * Implements possibly missing functions
 * and wrapper functions used by fSQL.
 */
 
// Portable recursive mkdir wrapper (recursive flag added in PHP 5)
function mkdir_recursive($pathname, $mode)
{
	return mkdir($pathname, $mode, true);
}

// Portable is_a() wrapper (uses instanceof)
function fsql_is_a($object, $classname)
{
	return $object instanceof $classname;
}
	
?>