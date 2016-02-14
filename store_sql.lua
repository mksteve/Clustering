module ("store_sql", package.seeall )

function StoreTable( data, metaData, schema_part, whole_schema, db )
    local stmt = db:prepare( schema_part.SQLInsert )
    for i,j in ipairs( schema_part.Fields ) do
        local val = data[j] or metaData[j]
        stmt:bind( i, val )
    end
    stmt:step()
    if schema_part.Type == "Main" then
        metaData[ "$IDX" ] = stmt:last_insert_rowid()
    end
end

function MakeSizeQuery( schema_part, data, metaData )
     local template = "INSERT INTO $(table_part) (ID, count ) SELECT ?, count(distinct word) FROM $(table) WHERE ID = ?;";
     local table = schema_part.Table .. "_Size"
     template = template:gsub("%$%(table%)", schema_part.Table )
     return template:gsub( "%$%(table_part%)", table )
end

function StoreArray( data, metaData, schema_part, whole_schema, db )
    local stmt = db:prepare( schema_part.SQLInsert )
    for k,v in ipairs( data ) do
        metaData[ "$KEY" ] = k
        metaData[ "$VALUE" ] = v
        for i,j in ipairs( schema_part.Fields ) do
            local val = data[j] or metaData[j]
            stmt:bind( i, val )
        end
        stmt:step()
        stmt:reset()
     end
     if schema_part.DoSize then
         local sizeQuery = MakeSizeQuery( schema_part, data, metaData )
         local stmt = db:prepare( sizeQuery  )
         stmt:bind( 1, metaData[ "$IDX" ] ) -- cope with zero entries - ensure query returns id.
         stmt:bind( 2, metaData[ "$IDX" ] )
         stmt:step()
     end
end


function StoreElement( data, metaData, schema_part, whole_schema, db )
    if schema_part.Type == "Array" then
        StoreArray( data, metaData, schema_part, whole_schema, db )
    elseif schema_part.Type == "Main" or
           schema_part.Type == "Table" then
        StoreTable( data, metaData, schema_part, whole_schema, db )
    else
        error( "Unknown Type for " .. schema_part.SQLInsert )
    end
end

function StoreSQL( data, metaData, schema, db )
    for i,j in ipairs( schema.Order ) do
        local dataPart = (schema[j].Part and data[ schema[j].Part] ) or data
        StoreElement( dataPart, metaData, schema[ j ], schema, db )
    end
end
