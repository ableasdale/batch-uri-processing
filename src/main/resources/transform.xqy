xquery version "1.0-ml";

declare variable $URI as xs:string external;

xdmp:node-insert-child(doc($URI)/test-data, element timestamp{fn:current-dateTime()})