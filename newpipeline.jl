include(joinpath("process_shufersal","fetch_storedata_shufersal.jl"))
include(joinpath("process_shufersal","fetch_files_shufersal.jl"))
include(joinpath("process_shufersal","process_files_shufersal.jl"))

include(joinpath("process_binaprojects","fetch_storedata_binaprojects.jl"))
include(joinpath("process_binaprojects","fetch_files_binaprojects.jl"))
include(joinpath("process_binaprojects","process_files_binaprojects.jl"))

include(joinpath("process_cerberus","fetch_storedata_cerberus.jl"))
include(joinpath("process_cerberus","fetch_files_cerberus.jl"))
include(joinpath("process_cerberus","process_files_cerberus.jl"))

import .ShufersalStoreData
import .ShufersalFetchFiles
import .ShufersalProcessFiles

import .BinaprojectsStoreData
import .BinaprojectsFetchFiles
import .BinaprojectsProcessFiles

import .CerberusStoreData
import .CerberusFetchFiles
import .CerberusProcessFiles

using DataFrames
using DataFramesMeta
using Serialization
using Dates

using Graphs
using StatsBase

using ProtoBuf
protojl("storeitems.proto", ".", "protobufs_folder")
include("protobufs_folder/storeitems_pb.jl")


RUN_TIME = Dates.format(Dates.now(),"yyyy-mm-ddTHH-MM-SS")
RUN_FOLDER = "GlobalRun$(RUN_TIME)"
mkdir(RUN_FOLDER)

try
    ShufersalStoreData.fetchStoreData(RUN_FOLDER)
    ShufersalFetchFiles.fetchFiles(RUN_FOLDER)
    ShufersalProcessFiles.processFiles(RUN_FOLDER)
catch e
    println("Error in Shufersal")
    println(e)
end

binaprojects_stores = ["kingstore","maayan2000","goodpharm","zolvebegadol","supersapir","citymarketgivatayim","citymarketkiryatgat","superbareket","ktshivuk","shuk-hayir","shefabirkathashem"]
for i in binaprojects_stores
    try
        BinaprojectsStoreData.fetchStoreData(i,RUN_FOLDER)
        BinaprojectsFetchFiles.fetchFiles(i,RUN_FOLDER)
        BinaprojectsProcessFiles.processFiles(i,RUN_FOLDER)
    catch e
        println("Error in $i")
        println(e)
    end
end

cerberus_stores_with_passwords = [
    ["doralon", ""],
    ["TivTaam", ""],
    ["yohananof", ""],
    ["osherad", ""],
    ["SalachD", "12345"],
    ["Stop_Market", ""],
    ["politzer", ""],
    ["Paz_bo", "paz468"],
    ["yuda_ho","Yud@147"],
    ["freshmarket", ""],
    ["Keshet", ""],
    ["RamiLevi", ""],
    ["SuperCofixApp", ""]
]

for i in cerberus_stores_with_passwords
    try
        CerberusStoreData.fetchStoreData(i[1],i[2],RUN_FOLDER)
        CerberusFetchFiles.fetchFiles(i[1],i[2],RUN_FOLDER)
        CerberusProcessFiles.processFiles(i[1],i[2],RUN_FOLDER)
    catch e
        println("Error in $i")
        println(e)
    end
end


# Unimplemented stores

# wolt https://wm-gateway.wolt.com/isr-prices/public/v1/index.html

# laibcatalog.co.il
# victory
# mahsanei shuk
# h cohen

# y bitan http://publishprice.ybitan.co.il/
# mega http://publishprice.mega.co.il/
# quik https://publishprice.quik.co.il/

# HaziHinam https://shop.hazi-hinam.co.il/Prices

# nativ hesed and barchal http://141.226.203.152/

# super-pharm http://prices.super-pharm.co.il/

# citymarket (except givatayim and kiryat gat) https://www.citymarket-shops.co.il/

# mishnat yosef https://chp-kt.pages.dev/

println("Finished fetching all data")


file_name_list = readdir(RUN_FOLDER)
file_list_stores = filter(x -> occursin(r"StoresTable",x),file_name_list)
file_list_prices = filter(x -> occursin(r"PricesTable",x),file_name_list)
file_list_promos = filter(x -> occursin(r"PromotionsTable",x),file_name_list)

# Process Stores Data

stores_table = DataFrame() 

for file in file_list_stores
    df = deserialize(joinpath(RUN_FOLDER, file))
    append!(stores_table, df; promote=true,cols=:union)
    df = nothing
end

store_data_message = storeitems_pb.StoresData(
    stores_table.StoreIds,
    stores_table.StoreLat,
    stores_table.StoreLon,
    stores_table.StoreNames	.* stores_table.StoreAddresses .* stores_table.StoreCities
    )

open(joinpath(RUN_FOLDER,"StoreData$(RUN_TIME).binpb"),"w") do f
    e = ProtoEncoder(f)
    ProtoBuf.encode(e, store_data_message)
end

stores_table = nothing
store_data_message = nothing

# Process Prices Data

prices_dataframe = DataFrame() 

for file in file_list_prices
    df = deserialize(joinpath(RUN_FOLDER, file))
    append!(prices_dataframe, df; promote=true,cols=:union)
    df = nothing
end

n = nrow(prices_dataframe)

g = SimpleGraph(n)

# Create dictionaries for itemid and name
itemid_dict = Dict{Any, Int}()
name_dict = Dict{Any, Int}()

for i in 1:n
    itemid = prices_dataframe[i, :ItemCode]
    name = prices_dataframe[i, :ManufacturerItemDescription] * string(prices_dataframe[i, :ItemName]) * string(prices_dataframe[i, :ItemNm])

    # If the itemid is already in the dictionary, add an edge
    if haskey(itemid_dict, itemid)
        add_edge!(g, itemid_dict[itemid], i)
    end
    itemid_dict[itemid] = i

    # If the name is already in the dictionary, add an edge
    if haskey(name_dict, name)
        add_edge!(g, name_dict[name], i)
    end
    name_dict[name] = i
end

components = connected_components(g) #85575

sort!(components, by=length, rev=true)

grouped_df = [prices_dataframe[component, :] for component in components]

# Preallocate arrays with known size
item_ids = Vector{Int64}(undef, length(grouped_df))
item_name = Vector{String}(undef, length(grouped_df))
item_pricing = Vector{Vector{Vector{Int64}}}(undef, length(grouped_df))

for (idx, dataframe) in enumerate(grouped_df)
    item_ids[idx] = dataframe.ItemCode[1]

    # Use a loop instead of creating a temporary array
    freq_dict = Dict{String, Int64}()
    for desc in dataframe.ManufacturerItemDescription
        for word in split(desc)
            freq_dict[word] = get(freq_dict, word, 0) + 1
        end
    end
    sorted_dict = sort(freq_dict, byvalue=true, rev=true)
    # get at most the first 5 words
    item_name[idx] = join(Iterators.take(keys(sorted_dict),5), " ")

    store_to_price_dict = Dict{Int64, Int64}()
    for i in range(1,nrow(dataframe))
        globalstoreid = parse(Int64,join([dataframe.StoreId[i],dataframe.ChainId[i]]))
        itemprice = round(Int64, dataframe.ItemPrice[i] * 100)

        if haskey(store_to_price_dict, itemprice) && store_to_price_dict[globalstoreid] > itemprice
            store_to_price_dict[globalstoreid] = itemprice
        else
            store_to_price_dict[globalstoreid] = itemprice
        end
    end

    item_pricing[idx] = [collect(keys(store_to_price_dict)), collect(values(store_to_price_dict))]
end

item_data_message = storeitems_pb.ItemData(
    item_ids,
    item_name,
    map(x->storeitems_pb.var"ItemData.ItemPricingData"(x[1],x[2]),item_pricing)
    )

open(joinpath(RUN_FOLDER,"ItemData$(RUN_TIME).binpb"),"w") do f
    e = ProtoEncoder(f)
    ProtoBuf.encode(e, item_data_message)
end

prices_dataframe = nothing
item_data_message = nothing

# Process Promotions Data

promotions_table = DataFrame() 

for file in file_list_promos
    df = deserialize(joinpath(RUN_FOLDER, file))
    append!(promotions_table, df; promote=true,cols=:union)
    df = nothing
end

# reward type 1 is a discount to a specific prices
# reward type 2 is a discount by a percentage
# reward type 10 is kinda everything but theres alot of them so its probably worth checking them out
@rsubset!(promotions_table,
        :ClubId == 0,
        :RewardType == 10 || :RewardType == 2 || :RewardType == 1,
        length(:PromotionItems) != 0,
        #Dates.now() <= DateTime(:PromotionEndDate)
        )

promotion_id = Vector{Int64}()
promotion_items = Vector{Vector{Int64}}()
promotion_stores = Vector{Vector{Int64}}()
promotion_end_time = Vector{Int64}()
promotion_discounted_price = Vector{Int64}()
promotion_discount_rate = Vector{Int64}()
promotion_description = Vector{String}()

for promo_group in @groupby(promotions_table, :PromotionId)
    push!(promotion_id,promo_group.PromotionId[1])

    union_of_items = reduce(union,promo_group.PromotionItems)
    push!(promotion_items,union_of_items)

    globalized_storeids = parse.(Int64,string.(promo_group.StoreId) .* string.(promo_group.ChainId))
    push!(promotion_stores,globalized_storeids)

    unixtime_of_promotion_end = Dates.datetime2unix(Date(promo_group.PromotionEndDate[1]) + Time(promo_group.PromotionEndHour[1]))
    push!(promotion_end_time,unixtime_of_promotion_end)

    # discounted price in agorot as Int64 0 if none
    discounted_price::Int64 = 0
    discounted_price_shown::Int64 = typemax(Int64)
    discounted_price_per_unit::Int64 = typemax(Int64)
    if !isnothing(promo_group.DiscountedPrice[1]) && !ismissing(promo_group.DiscountedPrice[1])
        discounted_price_shown = Int64(floor(parse(Float64,promo_group.DiscountedPrice[1])*100))
    end
    if !isnothing(promo_group.DiscountedPricePerMida[1]) && !ismissing(promo_group.DiscountedPricePerMida[1])
        discounted_price_per_unit = Int64(floor(parse(Float64,promo_group.DiscountedPricePerMida[1])*100))
    end
    # get smallest of the two values as long as they are not typemax
    result = min(discounted_price_shown,discounted_price_per_unit)
    if result != typemax(Int64)
        discounted_price = result
    end
    push!(promotion_discounted_price,discounted_price)

    # duscount rate in percentage with 1000 being equivalent to 100% off. if none equals 0
    discount_rate::Int64 = 0
    if !isnothing(promo_group.DiscountRate[1]) && !ismissing(promo_group.DiscountRate[1])
        discount_rate = promo_group.DiscountRate[1]
    end
    push!(promotion_discount_rate,discount_rate)

    # description of the promotion
    push!(promotion_description,promo_group.PromotionDescription[1])
end

promo_data_message = storeitems_pb.PromoData(
    promotion_id,
    map(x->storeitems_pb.var"PromoData.PromoItemIds"(x),promotion_items),
    map(x->storeitems_pb.var"PromoData.PromoStoreIds"(x),promotion_stores),
    promotion_end_time,
    promotion_discounted_price,
    promotion_discount_rate,
    promotion_description
    )

open(joinpath(RUN_FOLDER,"PromoData$(RUN_TIME).binpb"),"w") do f
    e = ProtoEncoder(f)
    ProtoBuf.encode(e, promo_data_message)
end

promotions_table = nothing
promo_data_message = nothing

println("Finished processing all data")

item_data_path = joinpath(RUN_FOLDER,"ItemData$(RUN_TIME).binpb")
run(`zstd $item_data_path -o ItemData.binpb.zst -f -22 --ultra --long`)

store_data_path = joinpath(RUN_FOLDER,"StoreData$(RUN_TIME).binpb")
run(`zstd $store_data_path -o StoreData.binpb.zst -f -22 --ultra --long`)

promo_data_path = joinpath(RUN_FOLDER,"PromoData$(RUN_TIME).binpb")
run(`zstd $promo_data_path -o PromoData.binpb.zst -f -22 --ultra --long`)

println("Finished compressing all data")
