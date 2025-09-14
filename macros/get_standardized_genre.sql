-- get_standardized_genre.sql
-- Macro to standardize genre names for consistent analysis

{% macro get_standardized_genre(genre_column) %}
    case
        when lower({{ genre_column }}) in ('rock', 'indie rock', 'alternative rock') then 'Rock'
        when lower({{ genre_column }}) in ('pop', 'indie pop', 'synthpop', 'electropop') then 'Pop'
        when lower({{ genre_column }}) in ('hip hop', 'rap', 'trap', 'drill') then 'Hip Hop'
        when lower({{ genre_column }}) in ('electronic', 'dance', 'edm', 'techno', 'house') then 'Electronic'
        when lower({{ genre_column }}) in ('jazz', 'free jazz', 'fusion') then 'Jazz'
        when lower({{ genre_column }}) in ('classical', 'orchestral', 'chamber') then 'Classical'
        when lower({{ genre_column }}) in ('folk', 'indie folk', 'singer-songwriter') then 'Folk'
        when lower({{ genre_column }}) in ('country', 'alt-country', 'americana') then 'Country'
        when lower({{ genre_column }}) in ('r&b', 'soul', 'neo-soul') then 'R&B'
        when lower({{ genre_column }}) in ('metal', 'heavy metal', 'black metal', 'death metal') then 'Metal'
        when lower({{ genre_column }}) in ('punk', 'hardcore', 'post-punk') then 'Punk'
        when lower({{ genre_column }}) in ('reggae', 'dub', 'dancehall') then 'Reggae'
        when lower({{ genre_column }}) in ('blues', 'rhythm and blues') then 'Blues'
        when lower({{ genre_column }}) in ('world', 'world music', 'global') then 'World'
        when lower({{ genre_column }}) in ('experimental', 'avant-garde', 'noise') then 'Experimental'
        when lower({{ genre_column }}) in ('ambient', 'drone', 'minimal') then 'Ambient'
        when lower({{ genre_column }}) in ('funk', 'disco') then 'Funk'
        when lower({{ genre_column }}) in ('gospel', 'spiritual') then 'Gospel'
        when lower({{ genre_column }}) in ('latin', 'salsa', 'bossa nova') then 'Latin'
        when lower({{ genre_column }}) in ('new age', 'meditation') then 'New Age'
        else 'Other'
    end
{% endmacro %}
