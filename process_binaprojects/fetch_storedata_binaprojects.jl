module BinaprojectsStoreData
export fetchStoreData

#=
using Dates
using HTTP
using Inflate
using GZip
using URIs
using SHA
=#

using HTTP
using JSON
using ZipArchives
using EzXML
using DataFrames
using CSV
using Serialization




function fetchStoreData(STORE_NAME::String,GLOBAL_FOLDER::String)
    println("Fetching $STORE_NAME data from binaprojects...")

    # Fetch file entries from binaprojects
    resp = HTTP.get("https://$STORE_NAME.binaprojects.com/MainIO_Hok.aspx")
    body_file_entries = JSON.parse(String(resp.body))

    # find latest stores file
    stores_file_entries = filter(x->occursin("Stores",x["FileNm"]),body_file_entries)
    store_list_xml_url = "https://$STORE_NAME.binaprojects.com/Download/$(stores_file_entries[1]["FileNm"])"

    # fetch stores file
    r = HTTP.get(store_list_xml_url)
    archive = ZipBufferReader(r.body)
    data = zip_readentry(archive,1,String)
    xml = parsexml(data)

    # parse into stores dataframe
    chainId = findfirst("//ChainId",xml.root).content

    storeid_nodes = findall("//StoreId",xml.root)
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