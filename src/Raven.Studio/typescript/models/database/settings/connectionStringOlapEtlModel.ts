﻿/// <reference path="../../../../typings/tsd.d.ts"/>
import connectionStringModel = require("models/database/settings/connectionStringModel");
import jsonUtil = require("common/jsonUtil");
import s3Settings = require("viewmodels/database/tasks/destinations/s3Settings");
import localSettings = require("models/database/tasks/periodicBackup/localSettings");
import getBackupLocationCommand = require("commands/database/tasks/getBackupLocationCommand");
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");
import generalUtils = require("common/generalUtils");
import getFolderPathOptionsCommand = require("commands/resources/getFolderPathOptionsCommand");
import database = require("models/resources/database");
import saveConnectionStringCommand = require("commands/database/settings/saveConnectionStringCommand");

class connectionStringOlapEtlModel extends connectionStringModel {

    s3Settings = ko.observable<s3Settings>();
    localSettings = ko.observable<localSettings>();
    
    destinationsChecked: KnockoutComputed<boolean>;
    
    validationGroup: KnockoutValidationGroup;
    testConnectionValidationGroup: KnockoutValidationGroup;

    locationInfo = ko.observableArray<Raven.Server.Web.Studio.SingleNodeDataDirectoryResult>([]);
    folderPathOptions = ko.observableArray<string>([]);

    spinners = {
        locationInfoLoading: ko.observable<boolean>(false)
    };
    
    dirtyFlag: () => DirtyFlag;

    constructor(dto: Raven.Client.Documents.Operations.ETL.OLAP.OlapConnectionString, isNew: boolean, tasks: { taskName: string; taskId: number }[]) {
        super(isNew, tasks);
        
        this.update(dto);
        
        this.initObservables();
        this.initValidation();
    }

    update(dto: Raven.Client.Documents.Operations.ETL.OLAP.OlapConnectionString) {
        super.update(dto);
        
        this.connectionStringName(dto.Name);
        this.s3Settings(dto.S3Settings ? new s3Settings(dto.S3Settings, null) : s3Settings.empty(null));
        this.localSettings(dto.LocalSettings ? new localSettings(dto.LocalSettings, "connectionString") : localSettings.empty("connectionString"));
    }
    
    initObservables() {
        const folderPath = this.localSettings().folderPath();
        if (folderPath) {
            this.updateLocationInfo(folderPath);
        }

        this.updateFolderPathOptions(folderPath);

        this.dirtyFlag = new ko.DirtyFlag([
            this.connectionStringName,
            this.s3Settings().dirtyFlag().isDirty,
            this.localSettings().dirtyFlag().isDirty
        ], false, jsonUtil.newLineNormalizingHashFunction);

        this.localSettings().folderPath.throttle(300).subscribe((newPathValue) => {
            if (this.localSettings().folderPath.isValid()) {
                this.updateLocationInfo(newPathValue);
                this.updateFolderPathOptions(newPathValue);
            } else {
                this.locationInfo([]);
                this.folderPathOptions([]);
                this.spinners.locationInfoLoading(false);
            }
        });

        this.destinationsChecked = ko.pureComputed(() => {
            const localEnabled = this.localSettings().enabled();
            const s3Enabled = this.s3Settings().enabled();
            return localEnabled || s3Enabled;
        });
    }

    initValidation() {
        super.initValidation();
        
        this.destinationsChecked.extend({
            validation: [
                {
                    validator: () => this.destinationsChecked(),
                    message: "Please select at least one destination"
                }
            ]
        });

        this.validationGroup = ko.validatedObservable({
            connectionStringName: this.connectionStringName,
            destinationsChecked: this.destinationsChecked
        });
    }
    
    static empty(): connectionStringOlapEtlModel { 
        return new connectionStringOlapEtlModel({
            Type: "Olap",
            Name: "",
            S3Settings: s3Settings.empty(null).toDto(),
            LocalSettings: localSettings.empty("connectionString").toDto()
        } , true, []);
    }
    
    toDto(): Raven.Client.Documents.Operations.ETL.OLAP.OlapConnectionString {
        return {
            Type: "Olap",
            Name: this.connectionStringName(),
            S3Settings: this.s3Settings().enabled() ? this.s3Settings().toDto() : undefined,
            LocalSettings: this.localSettings().enabled() ? this.localSettings().toDto() : undefined
        };
    }

    private updateLocationInfo(path: string) {
        const getLocationCommand = new getBackupLocationCommand(path, activeDatabaseTracker.default.database());

        const getLocationtask = getLocationCommand
            .execute()
            .done((result: Raven.Server.Web.Studio.DataDirectoryResult) => {
                if (this.localSettings().folderPath() !== path) {
                    // the path has changed
                    return;
                }

                this.locationInfo(result.List);
            });

        generalUtils.delayedSpinner(this.spinners.locationInfoLoading, getLocationtask);
    }

    private updateFolderPathOptions(path: string) {
        getFolderPathOptionsCommand.forServerLocal(path, true)
            .execute()
            .done((result: Raven.Server.Web.Studio.FolderPathOptions) => {
                if (this.localSettings().folderPath() !== path) {
                    // the path has changed
                    return;
                }

                this.folderPathOptions(result.List);
            });
    }

    saveConnectionString(db: database) : JQueryPromise<void> {
        return new saveConnectionStringCommand(db, this)
            .execute();
    }
}

export = connectionStringOlapEtlModel;
