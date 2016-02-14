local sqlite3 = require("lsqlite3")
local json= require( "LuaJSON_lib" )
require( "store_sql" )
require( "sql_help" )
require( "mtree" )
require( "EditDistance" )
require( "dbscan" )
dist = EditDistance.Distance("king", "sitting" )
local db = sqlite3.open( arg[1] )
db:execute( "PRAGMA cache_size = 80000;" )
da = EditDistanceExact( db, { 1,10}, "TwitterText", "Data" )
-- load schema
--dataFile = io.open( arg[2] , "r" )
--local schemaData = dataFile:read( "*a" )
--local schema = json.decode( schemaData )
--dataFile:close()
local schema = require( "schema" )
local reset = false


for i,j in ipairs( schema.Tables ) do
   db:execute( j )
end
for i,j in ipairs( schema.Indices ) do
   db:execute( j )
end
local schemajson = json.encode( schema )
dataFile = io.open( "schema.json" , "w" )
dataFile:write( schemajson )
dataFile:close()

local distance = {}

distance[#distance + 1] = MakeEditDistance("TwitterText", "data" , 1.4 )
distance[#distance + 1] = MakeJaccardDistance("TwitterRefers", "word" , .1 )
distance[#distance + 1] = MakeJaccardDistance("TwitterHashTags", "word" , .2 )


local tree = mtree.BuildMtree( db, 30, distance, reset )

if reset == true then
db:execute( "BEGIN;" )
local c = 0
local cnt = 0
local times = {}
function PrintTimes( cnt, times )
    local result = ""
    value = 100
    for i = 1 ,10 do
        local tst = value * 10 ^ (i - 1 )
        if cnt % tst == 0 then
            local newRec = { tm  = os.time() }
            if times[i] ~= nil then
                local res = string.format( "%d items at average %f  ", tst, (newRec.tm - times[i].tm) / tst )
                result = result .. res
            end
            times[i] = newRec
        end
    end
    if result ~= "" then
        print( cnt .. " items " .. result )
    end
end

for row in db:nrows("SELECT id FROM TwitterMain;" ) do
    c = c +1
    cnt = cnt + 1
    if cnt > 100 then
       PrintTimes( cnt, times )
    end
    if c >= 100 then
       db:execute( "COMMIT;" )
       db:execute( "BEGIN;" )
       c = 0
    end
    tree:InsertId(  row.ID )
end
db:execute( "COMMIT;" )
print( "Fixing radius" )
tree:FixRadius()

end
local a1 = CalcDistance( db, {11, 11 }, distance, false )
local e1 = CalcDistance( db, {11, 11 }, distance, true )


function RegionQuery( id, eps )
    local res = tree:SearchRadius( id, eps )
    return res.results
end
dbscan.DBSCAN( db, RegionQuery, "TwitterMain", .07, 5 )

for row in db:nrows("SELECT id FROM TwitterMain;" ) do
     print( row.ID, os.date() )
     local tStart = os.time()
     srcher = tree:SearchRadius( row.ID, .3 )
     for i,j in ipairs( srcher.results ) do
         print( i, j.id, j.dist )
     end
     local tMid = os.time()
     print( "====", srcher.exacts, srcher.approxs, os.date() )
     local stmt2 = db:prepare( "SELECT id FROM TwitterMain;" )
--     stmt2:bind( 1, row.ID )
     local srch2 = tree:MakeSearcher(  .3, nil )
     local approxs = 0
     local exacts = 0
     for test in stmt2:nrows() do
        do
            local range = srch2:GetRange()
            local dist = CalcDistance( db, {test.ID, row.ID }, distance, false )
            approxs = approxs + 1
            if range == nil or dist < range then
                exacts = exacts + 1
                dist = CalcDistance( db, {test.ID, row.ID }, distance, true ) -- calc exact
            end
            if range == nil or dist < range then
               srch2:AddResult( test.ID, dist )
            end
        end
     end
     for i,j in ipairs( srch2.results ) do
         print( i, j.id, j.dist )
     end
     local tEnd = os.time()
     print( "====",  exacts, approxs, os.date(), "DONE", os.difftime( tMid, tStart ), os.difftime( tEnd, tMid ) )    
     local res =  "====" .. srcher.exacts .. " " .. srcher.approxs .." ".. 
                             exacts .. " " .. approxs .. " " .. os.date() .. 
                             " DONE " .. os.difftime( tMid, tStart ) .. " " .. os.difftime( tEnd, tMid ) .. "\n" 
     io.stderr:write( res )
     io.flush()
end


local a = CalcDistance( db, {49846, 1993 }, distance, false )
local e = CalcDistance( db, {49846, 1993 }, distance, true )

for row in db:nrows("SELECT id FROM TwitterMain;" ) do
    local stmt2 = db:prepare( "SELECT id FROM TwitterMain WHERE ID <> ?;" )
    print( row.ID )
    stmt2:bind( 1, row.ID )
    for test in stmt2:nrows() do
        do
            local a = CalcDistance( db, {test.ID, row.ID }, distance, false )
            local e = CalcDistance( db, {test.ID, row.ID }, distance, true )
            local approx = JaccardApprox( db, {test.ID, row.ID}, "TwitterWords", "word" )
            local exact = JaccardExact( db, {test.ID, row.ID}, "TwitterWords", "word" )
            if( exact < .3 or approx > exact ) then
                print( test.ID, row.ID, " : " , approx, exact )
            end
        end
     end
end



print( "done" )
