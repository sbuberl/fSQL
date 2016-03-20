<?php

namespace FSQL;

class OrderByClause
{
    private $sortFunction;

    public function __construct(array $tosortData)
    {
        $code = '';
        foreach ($tosortData as $tosort) {
            $key = $tosort['key'];

            if ($tosort['ascend']) {
                $ltVal = -1;
                $gtVal = 1;
            } else {
                $ltVal = 1;
                $gtVal = -1;
            }

            if ($tosort['nullsFirst']) {
                $leftNullVal = -1;
                $rightNullVal = 1;
            } else {
                $leftNullVal = 1;
                $rightNullVal = -1;
            }

            $code .= <<<EOC
\$a_value = \$a[$key];
\$b_value = \$b[$key];
if(\$a_value === null)          return $leftNullVal;
elseif(\$b_value === null)      return $rightNullVal;
elseif(\$a_value < \$b_value)   return $ltVal;
elseif(\$a_value > \$b_value)   return $gtVal;
EOC;
        }
        $code .= 'return 0;';
        $this->sortFunction = create_function('$a, $b', $code);
    }

    public function sort(array &$data)
    {
        usort($data, $this->sortFunction);
    }
}
