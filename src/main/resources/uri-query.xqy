xquery version "1.0-ml";

declare variable $URI as xs:string external;

cts:uris( $URI, ("limit=1000"),
    cts:collection-query("/test-data")
)