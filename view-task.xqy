xquery version "1.0-ml";

import module namespace q = "http://grtjn.nl/marklogic/queue" at "queue-lib.xqy";

declare option xdmp:mapping "false";

let $id := xdmp:get-request-field("id")
where $id
return
	fn:doc(q:get-task-uri($id))