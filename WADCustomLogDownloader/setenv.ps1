Param(
	$SubscriontName,
	$StorageAccountName
)

Select-AzureSubscription -SubscriptionName $SubscriontName
$storageKey = Get-AzureStorageKey -StorageAccountName kinmuginl01
$env:AZURE_STORAGE_CONNECTION_STRING=("DefaultEndpointsProtocol=https;AccountName={0};AccountKey={1}" -f $StorageAccountName, $storageKey.Primary)