module ShufersalStoreData
export fetchStoreData

#=
using Dates
using JSON
using URIs
using SHA
=#

using HTTP
using EzXML
using Inflate
using DataFrames
using CSV
using Serialization


function fetchStoreData(GLOBAL_FOLDER::String)
    STORE_NAME = "Shufersal"
    #GLOBAL_FOLDER = "scratch_folder"

    @info "Fetching Shufersal Store data..."

    # fetch store list
    resp = HTTP.get("https://prices.shufersal.co.il/FileObject/UpdateCategory?catID=5&storeId=0")
    htmlstring = String(resp.body)
    htmlparsed = parsehtml(htmlstring)
    linkelements = findall("//tr/td/a[contains(@href, 'pricesprodpublic')]", htmlparsed.root)

    r = HTTP.get(linkelements[1]["href"])
    data = inflate_gzip(r.body) #TODO: This is the only place Inflate Jl is getting called. Not sure why it is used here instead of LibDeflate
    xml = parsexml(String(data))

    # parse into stores dataframe

    chainId = findfirst("//CHAINID",xml.root).content

    storeid_nodes = findall("//STOREID",xml.root)
    storeIds = [parse(Int64,(i.content * chainId)) for i in storeid_nodes]
    storename_nodes = findall("//STORENAME",xml.root)
    storeNames = [i.content for i in storename_nodes]
    storeaddress_nodes = findall("//ADDRESS",xml.root)
    storeAddresses = [i.content for i in storeaddress_nodes]
    storecity_nodes = findall("//CITY",xml.root)
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