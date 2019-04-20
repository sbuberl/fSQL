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
    private $types;
    private $boundParams;
    private $boundResults;
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

    public function close()
    {
        $this->query = null;
        $this->params = null;
        $this->result = null;
        $this->metadata = null;
        $this->types = null;
        $this->boundParams = null;
        $this->boundResults = null;
        $this->stored = null;
        $this->error = null;
        return true;
    }

    public function reset()
    {
        if($this->stored === false) {
            $this->result = null;
        }
        $this->error = null;
        return true;
    }

    public function data_seek($offset)
    {
        if($this->query === null) {
            return $this->set_error("Unable to perform a data_seek without a prepare");
        } elseif($this->stored === false) {
            return $this->set_error("Unable to perform a data_seek without a store_result");
        }

        $result = $this->result->dataSeek($offset);
        return $result !== false;
    }

    public function num_rows()
    {
        if($this->query === null) {
            return $this->set_error("Unable to perform a num_rows without a prepare");
        } elseif($this->stored === false) {
            return $this->set_error("Unable to perform a num_rows without a store_result");
        }

        return $this->result->numRows();
    }

    public function bind_param($types, &...$params)
    {
        $length = strlen($types);
        if($this->query === null) {
            return $this->set_error("Unable to perform a bind_param without a prepare");
        } elseif($length != count($params)) {
            return $this->set_error("bind_param's number of types in the string doesn't match number of parameters passed in");
        }

        $this->boundParams = $params;
        $this->types = $types;

        return true;
    }

    public function bind_result(&...$variables)
    {
        if($this->query === null) {
            return $this->set_error("Unable to perform a bind_result without a prepare");
        } elseif($this->result === null) {
            return $this->set_error("No result set found for bind_result");
        }

        $this->boundResults = $variables;
        return true;
    }

    public function execute()
    {
        if($this->query === null) {
            return $this->set_error("Unable to perform an execute without a prepare");
        }

        if($this->boundParams !== null) {
            $length = strlen($this->types);
            $param = reset($this->boundParams);

            $params = [];
            for($i = 0; $i < $length; ++$i, $param = next($this->boundParams)) {
                $type = $this->types[$i];
                if($param == null) {
                    $params[] = 'NULL';
                } else {
                    switch($type) {
                    case 'i':
                        $params[] = (int) $param;
                        break;
                    case 'd':
                        $params[] = (float) $param;
                        break;
                    case 's':
                    case 'o':
                        $params[] = "'". addslashes((string) $param) . "'";
                        break;
                    }
                }
            }
            $parameters = $params;
            $count = 0;
            $realQuery = preg_replace_callback(
                "/\?(?=[^']*(?:'[^']*'[^']*)*$)/",
                function($match) use ($parameters, &$count) { return $parameters[$count++]; },
                $this->query
                );
        } else {
            $realQuery = $this->query;
        }

        $result = $this->environment->query($realQuery);
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
        } elseif($this->boundResults === null) {
            return $this->set_error("Unable to perform a fetch without a bind_result");
        }

        $result = $this->result->fetchRow();
        if($result !== null) {
            $i = 0;
            foreach ($result as $value) {
                $this->boundResults[$i++] = $value;
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
        $this->types = null;
        $this->boundParams = null;
        $this->boundResults = null;
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
