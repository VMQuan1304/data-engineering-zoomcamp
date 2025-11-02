{% macro get_payment_type_description(payment_type) %}

    case {{ payment_type }}
        when 1 then 'Creadit Card'
        when 2 then 'Cash'
        when 3 then 'No Charge'
        when 4 then 'Dispute'
        when 5 then 'Unknow'
        when 6 then 'Voided Trip'
        else 'Empty'
    end

{% endmacro %}