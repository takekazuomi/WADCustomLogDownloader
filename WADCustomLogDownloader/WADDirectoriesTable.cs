﻿// -----------------------------------------------------------------------------------------
//    Copyright 2014 Takekazu Omi
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//      http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
// -----------------------------------------------------------------------------------------

using System;
using Microsoft.WindowsAzure.Storage.Table;

namespace WADCustomLogDownloader
{
    public class WadDirectoriesTable : TableEntity
    {
        public string DeploymentId { get; set; }
        public string Role { get; set; }
        public string RoleInstance { get; set; }
        public string SourceDirectory { get; set; }
        public DateTime FileTime { get; set; }
        public long FileSize { get; set; }
        public string CompleteFileName { get; set; }
        public string RelativePath { get; set; }
        public string Container { get; set; }
        public string Status { get; set; }
        public long EventTickCount { get; set; }
        public string RowIndex { get; set; }
        public DateTime TIMESTAMP { get; set; }
    }
}
