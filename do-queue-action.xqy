xquery version "1.0-ml";

import module namespace q = "http://grtjn.nl/marklogic/queue" at "queue-lib.xqy";

declare option xdmp:mapping "false";

declare variable $action external;
declare variable $id external;
declare variable $module external;
declare variable $prio as xs:integer external;

let $do :=
	if ($action eq "create") then
		q:create-task($module, $prio)
	else if ($action eq "remove") then
		q:delete-task($id)
	else if ($action eq "incprio") then
		q:set-task-prio($id, $prio + 1)
	else if ($action eq "decprio") then
		q:set-task-prio($id, $prio - 1)
	else if ($action eq "exec") then
		q:exec-task($id)
	else if ($action eq "start-cron") then
		q:start-cron(xdmp:random())
	else if ($action eq "stop-cron") then
		q:stop-cron()
	else if ($action eq "flush-task-server") then
		q:flush-task-server()
	else
		error(xs:QName("UNKNOWN-ACTION"), $action)
return
	fn:concat($action, " succesfull", if ($action eq 'stop-cron') then fn:concat(' (cron will stop within next ', $q:cron-sleep div 1000, ' sec)') else ())
