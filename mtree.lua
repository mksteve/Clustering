module( ..., package.seeall )
local json = require( "LuaJSON_lib" )

function BuildMtree( db, nodeCt, distance, reset, name )
   name = name or "_Main"
   function _S( sql )
      return SubField( sql, { name = name } )
   end
   local nodeCnt = nodeCt
   if reset then
       db:execute( _S("DROP TABLE IF EXISTS mtree_table_data$(name);") )
       db:execute( _S("DROP TABLE IF EXISTS mtree_table_json$(name);") )
   end
   
   db:execute( _S("CREATE TABLE IF NOT EXISTS mtree_table_data$(name) ( key TEXT PRIMARY KEY, value INTEGER );" ) )
   db:execute( _S("CREATE TABLE IF NOT EXISTS mtree_table_json$(name) ( ID INTEGER PRIMARY KEY AUTOINCREMENT, node TEXT);" ) )


   db:execute( _S("CREATE TABLE IF NOT EXISTS mtree_table_sql$(name) ( nodeId INTEGER, size INTEGER, leaf INTEGER, parentTreeId INTEGER, parentNodeId INTEGER);" ))
   db:execute( _S("CREATE TABLE IF NOT EXISTS mtree_table_sql_nodes$(name) ( nodeId INTEGER, idRecord INTEGER, idChild INTEGER);" ) )
   db:execute( _S("CREATE INDEX IF NOT EXISTS idx_mtree_table_sql$(name) ON mtree_table_sql$(name) ( nodeId );" ) )
   db:execute( _S("CREATE INDEX IF NOT EXISTS idx_mtree_table_sql_nodes$(name) ON mtree_table_sql_nodes$(name) ( nodeId );" ) )

   local nodeSize = db:prepare( _S("INSERT OR REPLACE INTO mtree_table_data$(name)( key, value) VALUES ( 'NodeSize', ? );" ) )
   nodeSize:bind(1, nodeCnt )
   nodeSize:step()


   local function newNode( parent, parentDataId, isLeaf)
       local res = {
           parent = parent,
           parentDataId = parentDataId,
           isLeaf = isLeaf,
           nodes = {
           }
       }
       if res.isLeaf == nil then res.isLeaf = true end
       return res
   end
    local function StoreNode( db, ndData, ndId )
        local jsonVal = json.encode( ndData )
        local storeStatement
        if ndId then
            storeStatement = db:prepare( _S( "UPDATE mtree_table_json$(name) SET node=? WHERE ID = ?;" ) )
            storeStatement:bind( 1, jsonVal );
            storeStatement:bind( 2, ndId );
            storeStatement:step();    
            return ndId
        else
            storeStatement = db:prepare( _S( "INSERT INTO mtree_table_json$(name) (node) VALUES (?);" ) )
            storeStatement:bind( 1, jsonVal );
            storeStatement:step();    
            return db:last_insert_rowid()
        end
    end

    local function LoadNode( db, ndId )
        local stmt = db:prepare( _S( "SELECT ID, node FROM mtree_table_json$(name) WHERE ID = ?;" ) )
        stmt:bind( 1, ndId );
        for i in stmt:nrows() do
            return json.decode( i.node )
        end
        error( "Broken tree" )
    end

    local function SetRoot( db, id )
       local root = db:prepare( _S( "INSERT OR REPLACE INTO mtree_table_data$(name)( key, value) VALUES ( 'root', ? );" ) )
       root:bind(1, id )
       root:step()

    end
    local function GetRoot( db )

       for rw in db:nrows( _S("SELECT key, value FROM mtree_table_data$(name) WHERE key = 'root';") ) do
          return rw.value
       end
       -- need to build a new object
       local nd = newNode()
       local id = StoreNode( db, nd )
       SetRoot( db, id )
       return id
    end

    local function MakeNodeEntry( db, dta, distanceTable, id, child, radius )
       local dist_parent = nil
       if dta.parentDataId then
           dist_parent = CalcDistance( db, {id, dta.parentDataId}, distanceTable, true ) 
       end
       local res = { child = child, id = id, dist_parent = dist_parent , radius = radius }
       return res
    end

    local function Promote( db, dta ,distanceTable )
        local matrix = {}
        for i,j in ipairs( dta.nodes ) do
             matrix[i]  = { id = j.id, distances = {} }
             for k,l in ipairs( dta.nodes ) do
                  if k == i then
                     matrix[i].distances[k] = { dist = 0, idx = k }
                  elseif k < i then
                      local tmp = matrix[k].distances[i]
                      matrix[i].distances[k] = { dist = tmp.dist, idx = k }
                  else
                      matrix[i].distances[k] = { dist = CalcDistance( db, {j.id, l.id }, distanceTable, true ), idx = k }
                  end
             end
        end
        -- now have built a set of tables, showing how far, choose 2 pairs which most evenly split the nodes.
        local best = nil
        local tmp1 = math.floor( #dta.nodes / 2 )
        local tmp2 = #dta.nodes - tmp1
        local max = tmp1 * tmp2
        for i, j in ipairs( matrix ) do
           for k,l in ipairs( matrix ) do
               if i >= k then -- nop, already considered.
               else -- i ~= j
                   local pos1 = 1
                   local pos2 = 1
                   local used1 = {}
                   local used2 = {}
                   for test = 1 , #dta.nodes do
                       if test == i then
                          used1[ #used1 + 1] = { idx = test, dist = matrix[i].distances[test].dist, id = dta.nodes[ test].id, node = dta.nodes[test] }
                       elseif test == k then
                          used2[ #used2 + 1] = { idx = test, dist = matrix[k].distances[test].dist, id = dta.nodes[ test].id, node = dta.nodes[test] }
                       elseif matrix[i].distances[ test].dist < matrix[k].distances[ test].dist then
                          used1[ #used1 + 1] = { idx = test, dist = matrix[i].distances[test].dist, id = dta.nodes[ test].id, node = dta.nodes[test] }
                       elseif matrix[i].distances[ test].dist > matrix[k].distances[ test].dist then
                          used2[ #used2 + 1] = { idx = test, dist = matrix[k].distances[test].dist, id = dta.nodes[ test].id, node = dta.nodes[test] }
                       elseif #used1 < #used2 then
                           used1[ #used1 + 1] = { idx = test, dist = matrix[i].distances[test].dist, id = dta.nodes[ test].id, node = dta.nodes[test] }
                       else
                           used2[ #used2 + 1] = { idx = test, dist = matrix[k].distances[test].dist, id = dta.nodes[ test].id, node = dta.nodes[test] }
                       end
                   end
                   local result = #used1 * #used2
                   if best == nil or result > best.result then
                       best = {id1 = i, id2 = k, result = result, pos1 = used1, pos2 = used2 }
                   end
                   if best.result == max then 
                       return best
                   end
               end
           end
        end
        return best
    end
    local splitCount = 0

    local function ValidateBranch( db, id )
        local dta = LoadNode( db, id )

        if dta.isLeaf == false then 
            for i, j in pairs( dta.nodes ) do
                local child = LoadNode( db, j.child )
                if( child.parent ~= id or
                    child.parentDataId ~= j.id ) then
                    print( "Failed ", splitCount )
                    error( "Failed at splitCount " .. tostring( splitCount ) )
                end
                ValidateBranch( db, j.child )
            end
        end
    end
    local function ValidateTree( db )
        if splitCount % 500 == 0 then
            local CurrentNode = GetRoot( db ) 
            ValidateBranch( db, CurrentNode )
        end
    end
    local function GetRadius( db, nodeId, baseId, distanceTable )
        local dta = LoadNode( db, nodeId )
        local radius = 0
        local node = LoadNode( db, nodeId )
        if node.isLeaf == false then
            for i, j in pairs ( dta.nodes ) do
                local chk = GetRadius( db, j.child, baseId, distanceTable )
                if chk > radius then
                   radius = chk
                end
            end
        else
            for i, j in pairs( dta.nodes ) do
                local chk = CalcDistance( db, { baseId, j.id}, distanceTable, true ) 
                if chk > radius then
                   radius = chk
                end
            end
        end
        return radius
    end
    local function FixNodeRadius( db, id, distanceTable, depth )
        local dta = LoadNode( db, id )
        if dta.isLeaf == false then
            for i, j in ipairs( dta.nodes ) do
                local radius = GetRadius( db, j.child, j.id, distanceTable )
                j.radius = radius
                FixNodeRadius( db, j.child, distanceTable, depth + 1 )
            end
            StoreNode( db, dta, id )
        end
        
    end
    local function FixRadius( self )
       local CurrentNode = GetRoot( self.db ) 
       FixNodeRadius( self.db, CurrentNode, self.distance, 1 )

    end
    local function FixChildrenToNewLocation( db, node, location )
        if node.isLeaf == false then 
            for i,j in pairs( node.nodes ) do
                local chld = LoadNode( db, j.child )
                chld.parent = location
                StoreNode( db, chld, j.child )
            end
        end        
    end

    local function Split( db, NodeId, Object , distanceTable)
        local ObjectId = Object.id
        splitCount = splitCount + 1
        local dta = LoadNode( db, NodeId )
        dta.nodes[ #dta.nodes + 1] = MakeNodeEntry( db, dta,distanceTable, ObjectId, Object.child, Object.radius ) -- patch current node to include new value

        local Op = dta.parentDataId
        local Np = dta.parent

        local res = Promote(db, dta, distanceTable ) -- worked out two nodes which try and split the data.
        local Op1 = dta.nodes[ res.id1 ]
        local Op2 = dta.nodes[ res.id2 ]
        local myOldNode = newNode( Np, Op, dta.isLeaf) -- will replace dta.
        local myNewNode = newNode( Np, Op, dta.isLeaf)

        local newRadius1 = 0
        local newRadius2 = 0
        
        for i,j in ipairs( res.pos1 ) do
            myOldNode.nodes[ #myOldNode.nodes + 1] = { child = j.node.child, id = j.id, dist_parent = j.dist, radius = j.node.radius }
            if (j.node.radius  or 0 ) + j.dist > newRadius1 then
                newRadius1 = (j.node.radius or 0 )+ j.dist
            end
        end
        for i,j in ipairs( res.pos2 ) do
            myNewNode.nodes[ #myNewNode.nodes + 1] = { child = j.node.child, id = j.id, dist_parent = j.dist, radius = j.node.radius }
            if (j.node.radius  or 0 ) + j.dist > newRadius2 then
                newRadius2 = (j.node.radius or 0 )+ j.dist
            end
        end
        if Np == nil then -- was the parent.
            local NewRoot = newNode( nil, nil, false )
            local newNode = MakeNodeEntry( db, NewRoot, distanceTable, Op1.id, NodeId, newRadius1 )
            NewRoot.nodes[ #NewRoot.nodes + 1 ] = newNode

            local loc = StoreNode( db, NewRoot )

            myNewNode.parent = loc
            myNewNode.parentDataId = Op2.id
            
            local newNodeLocation = StoreNode( db, myNewNode )

            FixChildrenToNewLocation( db, myNewNode, newNodeLocation )
            
            newNode = MakeNodeEntry( db, NewRoot, distanceTable, Op2.id, newNodeLocation, newRadius2 )
            NewRoot.nodes[ #NewRoot.nodes + 1 ] = newNode

            myOldNode.parent = loc
            myOldNode.parentDataId = Op1.id
            
            StoreNode( db, NewRoot, loc )
            StoreNode( db, myOldNode, NodeId )
            SetRoot( db, loc )
        else 
            -- update parent with oldnode => newnode.
            local parent = LoadNode( db, Np )
            for i,j in ipairs( parent.nodes ) do
                if j.child == NodeId then
                    j.id = Op1.id
                    local dist_parent
                    if parent.parentDataId then
                        dist_parent = CalcDistance( db, {Op1.id, parent.parentDataId}, distanceTable, true ) 
                    end
                    j.child = NodeId
                    j.dist_parent = dist_parent
                    j.radius = newRadius1
                    break
                end
            end
            StoreNode( db, parent, Np )
            myOldNode.parent = Np
            myOldNode.parentDataId = Op1.id        

            StoreNode( db, myOldNode, NodeId )

            myNewNode.parent = Np
            myNewNode.parentDataId = Op2.id        
            
            local newNodeLocation = StoreNode( db, myNewNode )
            FixChildrenToNewLocation( db, myNewNode, newNodeLocation )
            if #parent.nodes < nodeCnt then
               parent.nodes[ #parent.nodes + 1 ] = MakeNodeEntry( db, parent, distanceTable, Op2.id, newNodeLocation, newRadius2 )
               StoreNode( db, parent, Np )
            else
               Split( db, Np , { id = Op2.id, child = newNodeLocation, radius = newRadius2 } , distanceTable )
            end
        end
        ValidateTree( db )
    end

    local function InsertId( self, id )
--        print( "Inserting " .. id )
        local db = self.db 
        local distanceTable = self.distance
        local CurrentNode = GetRoot( db ) 
        local dta = LoadNode( db, CurrentNode )
        --- find the leaf
        while( dta.isLeaf == false ) do
           local best = nil
           for i, j in ipairs( dta.nodes ) do
              local dist = CalcDistance( db, {id, j.id}, distanceTable, true ) -- calculate exact distance
              if best == nil or dist < best.distance then
                 best = { new_Node = j.child, distance = dist, centre = j.id }
              end
           end
           if best ~= nil then
--               print( best.new_Node, best.distance, best.centre )
               CurrentNode = best.new_Node
               dta = LoadNode( db, CurrentNode )
           else
               error( "TBD" )
           end
        end
        if dta.isLeaf == true then
            if #dta.nodes < nodeCnt then
               local Nd = MakeNodeEntry( db, dta,distanceTable, id, nil )
               dta.nodes[ #dta.nodes + 1 ] = Nd
               StoreNode( db, dta, CurrentNode )
            else
               Split( db, CurrentNode, {id = id }, distanceTable )
            end
        end
    end
    local function lclMakeSearcher( db, radius, k, distanceTable )
        local searcher = {
            db     = db,
            kLimit = k,
            radius = radius,
            distanceTable = distanceTable,
            results = {},
            searchNodes = {},
            Load = function( self, id )
                return LoadNode( db, id )
            end,
            GetRange = function( self )
               if self.radius then return radius end
               if #self.results ~= self.kLimit then return nil end
               return self.currentLimit
            end,
            AddResult = function ( self, id, dist )
               self.results[ #self.results + 1] = { id = id, dist = dist }
               if self.kLimit and #self.results > self.kLimit then
                   table.sort( self.results, function( a,b ) if a.dist < b.dist then return true end return false end )
                   table.remove( self.results )
                   self.currentLimit = self.results[ self.kLimit ].dist
               end
            end,
            Distance = function( self, ids, exact )
                if exact then
                    self.exacts = self.exacts + 1
                    return CalcDistance( self.db, ids, self.distanceTable, exact )
                else
                    self.approxs = self.approxs + 1
                    return CalcDistance( self.db, ids, self.distanceTable, exact )
                end
            end,
            approxs = 0,
            exacts = 0,
            
        }
        if      k then searcher.radius = nil end
        if radius then searcher.k = nil end
        
        return searcher
    end

    local function InnerSearch( id, searcher )
       while #searcher.searchNodes > 0 do
            
           local searchNode = searcher.searchNodes[ #searcher.searchNodes] 
           local idN = searchNode.id
           table.remove( searcher.searchNodes )
           local node = searcher:Load( idN )
           local rq = searcher:GetRange()
           local distQToParent =  searchNode.distParent
           if node.isLeaf == false then
               local level = searchNode.level
               for i,j in ipairs( node.nodes ) do
                   local compute = true
                   if distQToParent and rq then
                       if math.abs( distQToParent - j.dist_parent ) > rq + j.radius then
                          compute = false
                       end
                   end
                   local dist = nil
                   if compute == true then
                       dist = searcher:Distance( {id, j.id},  true )
                   end
                   if dist ~= nil and (rq == nil or dist  < rq + j.radius ) then
                      searcher.searchNodes[ #searcher.searchNodes + 1] = { id = j.child, distParent = dist , level = level + 1 }
                   end
               end
                table.sort( searcher.searchNodes, function( a,b ) 
                           if a.level < b.level then
                               return true
                           elseif a.level > b.level then
                               return false
                           elseif a.distParent > b.distParent then
                               return true
                           else
                               return false
                           end
                       end)
           else
               for i,j in ipairs( node.nodes ) do
                   local compute = true
                   if distQToParent and rq then
                       if math.abs( distQToParent - j.dist_parent ) > rq  then
                          compute = false
                       end
                   end
                   if compute == true then
--                       if j.id == id and id == 11 then
--                           print( "." )
--                       end
                       local dist = searcher:Distance(  {id, j.id},  false )
                       if( rq == nil or dist < rq ) then
                           dist = searcher:Distance( {id, j.id},  true )
                       end
                       if rq == nil or dist < rq then
                          searcher:AddResult( j.id, dist )
                          rq = searcher:GetRange()
                       end
                   end
               end       
           end
       end
    end
    local function DoSearch( id, searcher )
        local rootId = GetRoot( searcher.db )
        searcher.searchNodes[ #searcher.searchNodes + 1] = { id = rootId, distParent = nil, level = 0 }
        InnerSearch( id, searcher )
    end

    local function SearchNearest( self, id, k)
         searcher = lclMakeSearcher( self.db, nil, k, self.distance)
         DoSearch( id, searcher )
         return searcher
    end

    local function SearchRadius( self, id, r )
         searcher = lclMakeSearcher(self.db, r, nil, self.distance )
         DoSearch( id, searcher )
         return searcher
    end


    local function MakeTree( db, distanceTable )
        return {
            Insert = function( id )
                return InsertId( db, distanceTable, id )
            end,
        }
    end
    local tbl = {
        db = db,
        name = name,
        nodeCnt = nodeCnt,
        distance = distance,
        InsertId = InsertId,   
        SearchNearest = SearchNearest,  
        SearchRadius = SearchRadius,
        FixRadius = FixRadius,
        MakeSearcher = function ( self, radius, k )
            return lclMakeSearcher( self.db, radius, k, self.distance ) 
        end,
    }    
    return tbl
end
