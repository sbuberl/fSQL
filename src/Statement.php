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
    private $paramCount;
    private $affectedRows;
    private $insertId;
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
        $this->paramCount = null;
        $this->affectedRows = null;
        $this->insertId = null;
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

    public function param_count()
    {
        return $this->paramCount;
    }

    public function affected_rows()
    {
        return $this->affectedRows;
    }

    public function insert_id()
    {
        return $this->insertId;
    }

    public function field_count()
    {
        if($this->query === null) {
            return $this->set_error("Unable to perform a field_count without a prepare");
        }

        return $this->result->field_count();
    }

    public function data_seek($offset)
    {
        if($this->query === null) {
            return $this->set_error("Unable to perform a data_seek without a prepare");
        } elseif($this->stored === false) {
            return $this->set_error("Unable to perform a data_seek without a store_result");
        }

        $result = $this->result->data_seek($offset);
        return $result !== false;
    }

    public function num_rows()
    {
        if($this->query === null) {
            return $this->set_error("Unable to perform a num_rows without a prepare");
        } elseif($this->stored === false) {
            return $this->set_error("Unable to perform a num_rows without a store_result");
        }

        return $this->result->num_rows();
    }

    public function bind_param($types, &...$params)
    {
        $length = strlen($types);
        $count = count($params);
        if($this->query === null) {
            return $this->set_error("Unable to perform a bind_param without a prepare");
        } elseif($length != $count) {
            return $this->set_error("bind_param's number of types in the string doesn't match number of parameters passed in");
        } elseif($this->paramCount != $count) {
            return $this->set_error("bind_param's number of params doesn't match number of params found in the query");
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
        elseif($this->paramCount != 0 && $this->boundParams === null) {
            return $this->set_error("Found parameters the in query without a calling bind_param");
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
            $this->metadata = $result->create_metadata();
            return true;
        } else if($result === false) {
            return $this->set_error(trim($this->environment->error()));
        } else {
            $this->affectedRows = $this->environment->affected_rows();
            $this->insertId = $this->environment->insert_id();
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

        $result = $this->result->fetch_row();
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
        if($query !== null && preg_match_all("/\?(?=[^']*(?:'[^']*'[^']*)*$)/", $query, $params)) {
            $this->paramCount = count($params[0]);
        } else {
            $this->paramCount = 0;
        }
        $this->params = [];
        $this->result = null;
        $this->metadata = null;
        $this->types = null;
        $this->boundParams = null;
        $this->boundResults = null;
        $this->affectedRows = 0;
        $this->insertId = 0;
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
