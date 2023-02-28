import { BlobStorageContainerConfig } from "./blob-storage-container-config";

export type BlobStorageConfig = {
    blobStorageConnectionString: string;
    storageAccountAccessKey: string;
    storageAccountName: string; 
    containerConfig : BlobStorageContainerConfig   
}