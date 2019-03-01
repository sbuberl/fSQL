<?php

namespace FSQL;

use FSQL\Environment;

class Statement
{
    private $environment;
    private $query;
    private $params;
    private $result;
    private $stored;

    public function __construct(Environment $environment, $query)
    {
        $this->environment = $environment;
        $this->prepare($query);
    }

    public function bind_param($types, ...$params)
    {
        $this->params = [];
        $param = current($params);

        $length = strlen($types);
        for($i = 0; $i < $length; ++$i, $param = next($params)) {
            $type = $types[$i];
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
        return true;
    }

    public function execute()
    {
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
          return true;
        } else {
          return $result;
        }
    }

    public function prepare($query)
    {
        $this->query = $query;
        $this->result = null;
        $this->stored = false;
        return true;
      }

    public function store_result()
    {
        $this->stored = true;
    }

    public function result_metadata()
    {
      return $this->result;
    }

    public function free_result()
    {

    }


}
