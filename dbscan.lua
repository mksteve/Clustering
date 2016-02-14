module( ..., package.seeall )
require( "sql_help" )

local function InitDBScan( db )
     --ResetDBScan( db )
     db:execute( "CREATE TABLE IF NOT EXISTS dbScanClusterData (clusterID INTEGER, id INTEGER, type INTEGER );" )
     db:execute( "CREATE TABLE IF NOT EXISTS dbScanClusterIds (clusterID INTEGER PRIMARY KEY AUTOINCREMENT, first INTEGER );" )
     db:execute( "CREATE INDEX IF NOT EXISTS idx_dbScanCluster_id ON dbScanClusterData (id );" );
     db:execute( "CREATE INDEX IF NOT EXISTS idx_dbScanCluster_clid ON dbScanClusterData ( clusterid );" );
     --db:execute( "DROP TABLE IF EXISTS dbScanScan;" );
     db:execute( "CREATE TABLE IF NOT EXISTS dbScanScan (id INTEGER PRIMARY KEY);" )
     db:execute( "CREATE INDEX IF NOT EXISTS idx_dbScanScan_id ON dbScanScan (id );" )
end

function ResetDBScan( db )
  db:execute( "DROP TABLE IF EXISTS dbScanClusterData;" )
  db:execute( "DROP TABLE IF EXISTS dbScanClusterIds;" )
  db:execute( "DROP TABLE IF EXISTS dbScanScan;" )

  
end

function AddToCluster( db, P, type, C )
    type = type or 0 -- 0 = centre, 1 = edge
    if C == nil then
       SqlHelper( { db = db, sql = "INSERT INTO dbScanClusterIds (first ) VALUES ( ?);", param = { P } } )
       C = db:last_insert_rowid();
    end
    SqlHelper( { db = db, sql = "INSERT INTO dbScanClusterData (clusterId, id, type ) VALUES( ?,?, ? );", param = { C, P, type} } )
    SqlHelper( {db = db, sql = "INSERT OR IGNORE INTO dbScanScan (id ) VALUES ( ? );", param = { P } } )

    return C
end
function DBSCAN( db, RegionQuery, tbl, eps, minPts )
     InitDBScan( db )

     local sql = "SELECT MIN(id) as nxt FROM $(table) WHERE ID NOT IN (SELECT id FROM dbScanScan);"
     sql = SubField( sql, { table = tbl } )
     
     local P
     repeat 
         P = SqlHelper( { db = db, sql = sql, param = {} , result = "nxt" } )
         if P ~= nil then
             print( "id    " .. tostring(P) .. "  " ..  os.date() .."                            " )
             
             -- mark P as visited
--             if P == 6 then
--                 print( "." )
--             end
             local neighbours = RegionQuery( P, eps )
             if #neighbours < minPts then
                SqlHelper( {db = db, sql = "INSERT OR IGNORE INTO dbScanScan (id ) VALUES ( ? );", param = { P } } )
             else
                 db:execute( "BEGIN;" );
                 C = AddToCluster( db, P )
                 expandCluster(db, P, neighbours, C, eps, minPts, RegionQuery )
                 db:execute( "COMMIT;" );
             end
         end
     until P == nil
end
function IsClustered( db, id )
    local i = SqlHelper( { db = db, sql = "SELECT id FROM dbScanClusterData WHERE id = ?;", param = { id } , result = "id" } )
    if i then return true end
    return false
end

function RemoveEntry( tb, id )
    for i,j in ipairs( tb ) do
       if j.id == id then
          table.remove( tb, i )
          return
       end
    end
end
function expandCluster( db, P, neigh, C, eps, minPts, RegionQuery )
    local neighbours = {}
    local visited = {}
    for i,j in ipairs( neigh ) do
        if j.dist == 0 and P ~= j.id then
            AddToCluster( db, j.id, 0, C )
            visited[ j.id ] = true
        else
            neighbours[ #neighbours + 1] = j
        end
    end
    for i,j in ipairs( neighbours ) do
       visited[ j.id ] = true
    end
    while #neighbours > 1 do
        io.write( "neighbours " .. tostring( #neighbours ) .. " " .. os.date() .."      \r" )
        local item = neighbours[ #neighbours ]
        table.remove( neighbours )
        local type = 1
        local newneighbours = {}
        if item.dist == 0 then 
            type = 0 -- only type zero added to the list
        else
            newneighbours = RegionQuery( item.id, eps )
        end
        if #newneighbours >= minPts then
            type = 0
           for i,j in ipairs( newneighbours ) do
               if visited[ j.id ] ~= true and j.dist > 0 then
                   neighbours[ #neighbours + 1] = j
                   visited[ j.id ] = true
               end
               if j.dist == 0 then
                    RemoveEntry( neighbours, j.id )
                    SqlHelper( {db = db, sql = "INSERT OR IGNORE INTO dbScanScan (id ) VALUES ( ? );", param = { j.id } } )
               end
            end
         end
        if not IsClustered( db, item.id ) then
            AddToCluster( db, item.id, type, C )
        end
    end

end


