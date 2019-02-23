<?php

namespace FSQL;

use FSQL\Environment;

class Query
{
    private $environment;
    private $query;
    private $positions;
    private $params;

    public function __construct(Environment $environment, $query, array $positions)
    {
        $this->environment = $environment;
        $this->query = $query;
        $this->positions = $positions;
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
        return $this->environment->query($newQuery);
    }
}
