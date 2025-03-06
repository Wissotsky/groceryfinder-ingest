module CerberusStoreData
export fetchStoreData

#=
using Inflate
using GZip
using URIs
using SHA
=#

using Dates
using EzXML
using HTTP
using JSON
using DataFrames
using CSV
using Serialization

function is_less_for_cerberus_file_time(x,y)
    fileTimeStringX = x["ftime"]
    if fileTimeStringX == "" fileTimeStringX = "01/01/1970 00:00" end
    fileDateTimeX = DateTime(fileTimeStringX,dateformat"mm/dd/yyyy HH:MM")
    fileTimeStringY = y["ftime"]
    if fileTimeStringY == "" fileTimeStringY = "01/01/1970 00:00" end
    fileDateTimeY = DateTime(fileTimeStringY,dateformat"mm/dd/yyyy HH:MM")
    return fileDateTimeX < fileDateTimeY
end

function cerberus_auth_to_token(username::String,password::String)::String
    # get the CSRF token for logging in
    resp = HTTP.post("https://url.publishedprices.co.il/login/user"; query=Dict("r" => "", "username" => username, "password" => password, "Submit" => "Sign+in", "csrftoken" => ""))
    htmlstring = String(resp.body)
    htmlparsed = parsehtml(htmlstring)

    # <meta name="csrftoken" content="PpDWa7TP7DOwoBp62SlOYNLSv2s9t0rWOGcYcIcFA3k"/>&#13;
    node = findfirst("""//meta[@name='csrftoken']""",htmlparsed.root)
    csrfToken = node["content"]

    # get csrf token of signed up account
    resp = HTTP.post("https://url.publishedprices.co.il/login/user"; query=Dict("r" => "", "username" => username, "password" => password, "Submit" => "Sign+in", "csrftoken" => csrfToken))
    htmlstring = String(resp.body)
    htmlparsed = parsehtml(htmlstring)

    node = findfirst("""//meta[@name='csrftoken']""",htmlparsed.root)
    csrfToken = node["content"]
    return csrfToken
end

function fetchStoreData(STORE_NAME::String, CERBERUS_PASSWORD::String, GLOBAL_FOLDER::String)

    println("Fetching $STORE_NAME data from Cerberus...")

    # Authenticate with cerberus
    csrf_token = cerberus_auth_to_token(STORE_NAME,CERBERUS_PASSWORD)

    if STORE_NAME == "yuda_ho"
        # fetch url for stores file
        resp = HTTP.post("https://url.publishedprices.co.il/file/json/dir"; query=Dict("iDisplayLength" => "100000", "sSearch" => "Stores", "csrftoken" => csrf_token, "cd" => "/Yuda"))
        body = JSON.parse(String(resp.body))
        # sort by file recency. from most recent to least recent
        json_file_list_sorted_by_recency = sort(body["aaData"],lt=is_less_for_cerberus_file_time,rev=true)
        # filter out files with NULL in their names
        filter!(x -> !occursin("NULL",x["name"]),json_file_list_sorted_by_recency)
        store_list_xml_url = "https://url.publishedprices.co.il/file/d/Yuda/$(json_file_list_sorted_by_recency[1]["name"])?dm=0"
        println("Fetching store list from $store_list_xml_url")
    else
        # fetch url for stores file
        resp = HTTP.post("https://url.publishedprices.co.il/file/json/dir"; query=Dict("iDisplayLength" => "100000", "sSearch" => "Stores", "csrftoken" => csrf_token))
        body = JSON.parse(String(resp.body))
        # sort by file recency. from most recent to least recent
        json_file_list_sorted_by_recency = sort(body["aaData"],lt=is_less_for_cerberus_file_time,rev=true)
        # filter out files with NULL in their names
        filter!(x -> !occursin("NULL",x["name"]),json_file_list_sorted_by_recency)
        store_list_xml_url = "https://url.publishedprices.co.il/file/d/$(json_file_list_sorted_by_recency[1]["name"])?dm=0"
        println("Fetching store list from $store_list_xml_url")
    end

    # fetch stores file
    r = HTTP.get(store_list_xml_url)
    bodyAsUInt16 = reinterpret(UInt16, r.body)
    bodyAsString = transcode(String, bodyAsUInt16)
    xml = parsexml(bodyAsString)

    # get cookie from response
    cookies = HTTP.Cookies.cookies(r)
    cftpsid_cookie = HTTP.Cookies.stringify(cookies[1])

    # parse into stores dataframe
    chainId = findfirst("//ChainId | //ChainID",xml.root).content

    storeid_nodes = findall("//StoreId | //StoreID",xml.root)
    storeIds = [parse(Int64,(i.content * chainId)) for i in storeid_nodes]
    storename_nodes = findall("//StoreName",xml.root)
    storeNames = [i.content for i in storename_nodes]
    storeaddress_nodes = findall("//Address",xml.root)
    storeAddresses = [i.content for i in storeaddress_nodes]
    storecity_nodes = findall("//City",xml.root)
    storeCities = [i.content for i in storecity_nodes]

    StoresDataframe = DataFrame(
        StoreIds = storeIds,
        StoreNames = storeNames,
        StoreAddresses = storeAddresses,
        StoreCities = storeCities,
        StoreLat = NaN64,
        StoreLon = NaN64,
    )


    # add preexisting locations
    knownStoreLocations = CSV.read("locations.csv",DataFrame)

    knownStoreLocations.StoreIds = parse.(Int64,string.(knownStoreLocations.StoreLocalId) .* string.(knownStoreLocations.ChainId))
    # Join the two dataframes on the StoreId key
    merged_df = leftjoin(StoresDataframe, knownStoreLocations, on = :StoreIds, makeunique=true)

    knownStoreLocations = nothing

    # Override the StoreLat and StoreLon values in the current dataframe with the values from the known locations dataframe
    merged_df.StoreLat = coalesce.(merged_df.StoreLat_1, merged_df.StoreLat)
    merged_df.StoreLon = coalesce.(merged_df.StoreLon_1, merged_df.StoreLon)

    StoresDataframe = select(merged_df, Not([:StoreLat_1, :StoreLon_1, :ChainId, :StoreLocalId]))

    merged_df = nothing
    # DataFrame with NaN values
    df_nan = StoresDataframe[isnan.(StoresDataframe.StoreLat), :]
    df_nan.StoreName .= STORE_NAME
    CSV.write(joinpath(GLOBAL_FOLDER,"UnknownStores.csv"),df_nan,append=true)

    # DataFrame without NaN values
    StoresDataframe = StoresDataframe[.!isnan.(StoresDataframe.StoreLat), :]

    println(df_nan)

    df_nan = nothing

    # save to disk

    println("Saving store data to disk")

    serialize("$GLOBAL_FOLDER/$STORE_NAME-StoresTable.df",StoresDataframe)

end

end