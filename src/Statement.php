<?php

namespace FSQL;

use FSQL\Environment;

class Statement
{
    private $environment;
    private $query;
    private $params;
    private $result;
    private $metadata;
    private $bound;
    private $stored;
    private $error;

    public function __construct(Environment $environment, $query = null)
    {
        $this->environment = $environment;
        $this->prepare($query);
    }

    public function error()
    {
        return $this->error;
    }

    private function set_error($error)
    {
        $this->error = $error;
        return false;
    }

    public function bind_param($types, ...$params)
    {
        $length = strlen($types);
        if($this->query === null) {
            return $this->set_error("Unable to perform a bind_param without a prepare");
        } elseif($length != count($params)) {
            return $this->set_error("bind_param's number of types in the string doesn't match number of parameters passed in");
        }

        $param = current($params);

        for($i = 0; $i < $length; ++$i, $param = next($params)) {
            $type = $types[$i];
            if($param == null) {
                $this->params[] = 'NULL';
            } else {
                switch($type) {
                case 'i':
                    $this->params[] = (int) $param;
                    break;
                case 'd':
                    $this->params[] = (float) $param;
                    break;
                case 's':
                case 'o':
                    $this->params[] = "'". addslashes((string) $param) . "'";
                    break;
                }
            }
        }

        return true;
    }

    public function bind_result(&...$variables)
    {
        if($this->query === null) {
            return $this->set_error("Unable to perform a bind_result without a prepare");
        } elseif($this->result === null) {
            return $this->set_error("No result set found for bind_result");
        }

        $this->bound = $variables;
        return true;
    }

    public function execute()
    {
        if($this->query === null) {
            return $this->set_error("Unable to perform an execute without a prepare");
        }

        $that = $this;
        $count = 0;
        $newQuery = preg_replace_callback(
            "/\?(?=[^']*(?:'[^']*'[^']*)*$)/",
            function($match) use ($that, &$count) { return $that->params[$count++]; },
            $this->query
            );
        $result = $this->environment->query($newQuery);
        if($result instanceof ResultSet) {
            $this->result = $result;
            $this->metadata = $result->createMetadata();
            return true;
        } else if($result === false) {
            return $this->set_error(trim($this->environment->error()));
        } else {
            return true;
        }
    }

    public function fetch()
    {
        if($this->query === null) {
            return $this->set_error("Unable to perform a fetch without a prepare");
        } elseif($this->bound === null) {
            return $this->set_error("Unable to perform a fetch without a bind_result");
        } elseif($this->result === null) {
            return $this->set_error("No result set found for fetch");
        }

        $result = $this->result->fetchRow();
        if($result !== null) {
            $i = 0;
            foreach ($result as $value) {
                $this->bound[$i++] = $value;
            }
            return true;
        }
        return null;
    }

    public function prepare($query)
    {
        $this->query = $query;
        $this->params = [];
        $this->result = null;
        $this->metadata = null;
        $this->bound = null;
        $this->stored = false;
        return true;
      }

    public function store_result()
    {
        if($this->query === null) {
            return $this->set_error("Unable to perform a store_result without a prepare");
        }

        $this->stored = true;
        return true;
    }

    public function get_result()
    {
        if($this->query === null) {
            return $this->set_error("Unable to perform a get_result without a prepare");
        }

        return $this->result;
    }

    public function result_metadata()
    {
        if($this->query === null) {
            return $this->set_error("Unable to perform a result_metadata without a prepare");
        }

        return $this->metadata;
    }

    public function free_result()
    {
        $this->result = null;
    }

}
