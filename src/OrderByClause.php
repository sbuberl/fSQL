<?php

namespace FSQL;

class OrderByClause
{
    private $sortFunction;

    public function __construct(array $tosortData)
    {
        $sortInfo = [];
        foreach ($tosortData as $tosort) {
            $key = $tosort['key'];

            if ($tosort['ascend']) {
                $ltVal = -1;
            } else {
                $ltVal = 1;
            }

            if ($tosort['nullsFirst']) {
                $leftNullVal = -1;
            } else {
                $leftNullVal = 1;
            }

            $sortInfo[] = [$key, $ltVal, $leftNullVal];
        }

        $this->sortFunction = function($a, $b) use ($sortInfo)
        {
            foreach($sortInfo as $info)
            {
                list($key, $ltVal, $leftNullVal) = $info;
                $aValue = $a[$key];
                $bValue = $b[$key];
                if($aValue === null)
                {
                    if($bValue == null)
                        continue;
                    else
                        return $leftNullVal;
                }
                elseif($bValue === null)    return -$leftNullVal;
                elseif($aValue < $bValue)   return $ltVal;
                elseif($aValue > $bValue)   return -$ltVal;
            }
            return 0;
        };
    }

    public function sort(array &$data)
    {
        usort($data, $this->sortFunction);
    }
}
