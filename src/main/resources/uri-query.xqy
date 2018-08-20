xquery version "1.0-ml";

declare variable $URI as xs:string external;

cts:uris( $URI, ("limit=100"),
    cts:collection-query("/test-data")
)