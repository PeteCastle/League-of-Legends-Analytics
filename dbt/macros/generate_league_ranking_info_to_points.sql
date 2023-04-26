{% macro generate_league_ranking_info_to_points()%}
    {# This list is hardcoded since the table is not officially available in Data Dragon.  The pointing system is per league, with 1 being the challenger and iron 4 = 27 (lowest)#}
    {% set league_pointing_system =
         ["CHALLENGER","I","1"],
        ["GRANDMASTER","I","2"],
        ["MASTER","I","3"],
        ["DIAMOND","I","4"],
        ["DIAMOND","II","5"],
        ["DIAMOND","III","6"],
        ["DIAMOND","IV","7"],
        ["PLATINUM","I","8"],
        ["PLATINUM","II","9"],
        ["PLATINUM","III","10"],
        ["PLATINUM","IV","11"],
        ["GOLD","I","12"],
        ["GOLD","II","13"],
        ["GOLD","III","14"],
        ["GOLD","IV","15"],
        ["SILVER","I","16"],
        ["SILVER","II","17"],
        ["SILVER","III","18"],
        ["SILVER","IV","19"],
        ["BRONZE","I","20"],
        ["BRONZE","II","21"],
        ["BRONZE","III","22"],
        ["BRONZE","IV","23"],
        ["IRON","I","24"],
        ["IRON","II","25"],
        ["IRON","III","26"],
        ["IRON","IV","27"],
    %}
    CASE
    {% for row in league_pointing_system %}
        WHEN tier = "{{ row[0] }}" AND division = "{{ row[1]}}" THEN {{ row[2] }}
    {% endfor %}
    ELSE NULL
    END AS player_league_point
{% endmacro %}