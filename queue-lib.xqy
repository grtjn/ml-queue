xquery version "1.0-ml";

module namespace q = "http://grtjn.nl/marklogic/queue";

declare namespace ss = "http://marklogic.com/xdmp/status/server";
declare namespace hs = "http://marklogic.com/xdmp/status/host";

declare option xdmp:mapping "false";

declare variable $q:root-uri := "/tasks/";
declare variable $q:collection := "queue";
declare variable $q:max-prio := 50;
declare variable $q:min-prio := - $q:max-prio;
declare variable $q:cron-lock-uri := fn:resolve-uri("cron-lock.xml", $q:root-uri);
declare variable $q:cron-stop-uri := fn:resolve-uri("cron-stop.xml", $q:root-uri);
declare variable $q:cron-sleep := 60 (: sec :) * 1000 (: ms :);

declare function q:flush-task-server()
{
	(: Thnx to Christopher Cieslinski from LDSChurch  for the inspiration.. :)
	
	let $host-id as xs:unsignedLong := xdmp:host()
	let $host-status := xdmp:host-status($host-id)
	let $task-server-id as xs:unsignedLong := $host-status//hs:task-server-id
	let $task-server-status := xdmp:server-status($host-id, $task-server-id)
	
	let $this-request-id as xs:unsignedLong := xdmp:request()

	let $task-ids as xs:unsignedLong* := $task-server-status//ss:request-id[. != $this-request-id]
	let $queue-size as xs:integer := $task-server-status//ss:queue-size
	return (
		if (fn:count($task-ids) gt 1) then
			for $id in $task-ids
			return
				try {
					xdmp:log(fn:concat("Cancelling task ", $id)),
					xdmp:request-cancel($host-id, $task-server-id, $id)
				} catch ($e) {
					xdmp:log(fn:concat("Failed to cancel task ", $id))
				}
		else
			xdmp:log("No tasks to cancel..")
		,
		if ($queue-size gt 1) then (
			xdmp:log("Queue not empty yet, trying again.."),
			xdmp:sleep(1000),
			q:flush-task-server()
		) else
			xdmp:log("Queue empty, done..")
	)      
};

declare function q:get-task-server-threads-available()
	as xs:integer
{
(:
	let $host-id as xs:unsignedLong := xdmp:host()
	let $host-status := xdmp:host-status($host-id)
	let $task-server-id as xs:unsignedLong := $host-status//hs:task-server-id
	let $task-server-status := xdmp:server-status($host-id, $task-server-id)
	let $task-server-threads as xs:integer := $task-server-status//ss:threads
	let $task-server-max-threads as xs:integer := $task-server-status//ss:max-threads
	return
		$task-server-max-threads - $task-server-threads
:)
	1 (: for testing :)
};

declare function q:get-task-uri($id)
{
	fn:concat($q:root-uri, $id, ".xml")
};

declare function q:create-task($module, $prio as xs:integer?)
	as empty-sequence()
{
	if (q:module-exists($module)) then
		let $id := xdmp:random()
		let $prio := if (fn:exists($prio)) then $prio else 0
		return
		xdmp:document-insert(q:get-task-uri($id),
			<q:task id="{$id}" created="{fn:current-dateTime()}">
				<q:module>{$module}</q:module>
				<q:prio>{$prio}</q:prio>
			</q:task>,
			xdmp:default-permissions(),
			$q:collection
		)
	else
		fn:error(xs:QName("MODULENOTFOUND"), fn:concat("Module ", $module, " not found in modules-database ", xdmp:modules-database()))
};

declare function q:delete-task($id)
	as empty-sequence()
{
	xdmp:document-delete(q:get-task-uri($id))
};

declare function q:set-task-prio($id, $prio as xs:integer)
	as empty-sequence()
{
	let $prio := fn:max( (fn:min( ($prio, 50) ), -50) )
	return
		xdmp:node-replace(fn:doc(q:get-task-uri($id))/q:task/q:prio, <q:prio>{$prio}</q:prio>)
};

declare function q:exec-task($id)
	as empty-sequence()
{
	let $task := fn:doc(q:get-task-uri($id))/q:task
	return (
		(: delete immediately :)
		xdmp:eval('
			xquery version "1.0-ml";
			import module namespace q = "http://grtjn.nl/marklogic/queue" at "queue-lib.xqy";
			q:delete-task($id)
		'),
		xdmp:spawn($task/q:module)
	)
};

declare function q:get-tasks($start, $page-size)
	as document-node()*
{
	let $end := $start + $page-size - 1
	return
	(
		for $task in
			cts:search(fn:collection($q:collection), cts:and-query(()))
		order by $task/q:task/q:prio ascending, xs:dateTime($task/q:task/@created) ascending
		return $task
		
	) [$start to $end]

};

declare function q:module-exists($module)
	as xs:boolean
{
	if (xdmp:modules-database() eq 0) then
		(: check on file-sys :)
		fn:exists(
			xdmp:document-get(
				fn:concat(fn:translate(xdmp:modules-root(), "\", "/"), $module),
				<options xmlns="xdmp:document-get"><format>text</format></options>
			)
		)
	else
		(: check in modules database :)
		xdmp:eval(fn:concat("fn:doc-available('", $module, "')"), (),
			<options xmlns="xdmp:eval">
				<database>{xdmp:modules-database()}</database>
			</options>
		)
};

declare function q:get-request-url()
{
	fn:concat(
		xdmp:get-request-protocol(),
		"://",
		xdmp:get-request-header("Host"),
		xdmp:get-request-url()
	)
};

declare function q:is-cron-active()
	as xs:boolean
{
	xdmp:eval(fn:concat("fn:doc-available('", $q:cron-lock-uri,"')"))
};

declare function q:claim-cron-lock()
{
	xdmp:eval(fn:concat("xdmp:document-insert('", $q:cron-lock-uri,"', <lock/>)"))
};

declare function q:release-cron-lock()
{
	xdmp:eval(fn:concat("xdmp:document-delete('", $q:cron-lock-uri, "')"))
};

declare function q:start-cron($id)
{
	if (q:should-cron-stop()) then
		q:deactivate-stop-sign()
	else (),
	xdmp:spawn("queue-cron.xqy", (xs:QName("id"), $id))
};

declare function q:stop-cron()
{
	q:activate-stop-sign()
};

declare function q:should-cron-stop()
	as xs:boolean
{
	xdmp:eval(fn:concat("fn:doc-available('", $q:cron-stop-uri,"')"))
};

declare function q:activate-stop-sign()
{
	xdmp:eval(fn:concat("xdmp:document-insert('", $q:cron-stop-uri,"', <stop/>)"))
};

declare function q:deactivate-stop-sign()
{
	xdmp:eval(fn:concat("xdmp:document-delete('", $q:cron-stop-uri, "')"))
};

