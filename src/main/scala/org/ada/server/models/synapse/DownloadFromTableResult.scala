package org.ada.server.models.synapse

import java.util.Date
import org.ada.server.json.EnumFormat
import play.api.libs.json.Json

// http://hud.rel.rest.doc.sagebase.org.s3-website-us-east-1.amazonaws.com/org/sagebionetworks/repo/model/asynch/AsynchJobState.html
object AsyncJobState extends Enumeration {
  val PROCESSING, FAILED,COMPLETE = Value
}

// http://hud.rel.rest.doc.sagebase.org.s3-website-us-east-1.amazonaws.com/org/sagebionetworks/repo/model/asynch/AsynchronousJobStatus.html
case class AsynchronousJobStatus(
  errorMessage: Option[String],
  progressMessage: Option[String],
  progressTotal: Option[Int],
  etag: String,
  jobId: String,
  errorDetails: Option[String],
  exception: Option[String],
  startedByUserId: Int,
  jobState: AsyncJobState.Value,
  progressCurrent: Option[Int],
  changedOn: Date,
  startedOn: Date,
  runtimeMS: Long,
  jobCanceling: Boolean
)

// http://hud.rel.rest.doc.sagebase.org.s3-website-us-east-1.amazonaws.com/org/sagebionetworks/repo/model/table/ColumnType.html
object ColumnType extends Enumeration {
  val STRING, DOUBLE, INTEGER, BOOLEAN, DATE, FILEHANDLEID, ENTITYID, LINK, LARGETEXT = Value
}

// http://hud.rel.rest.doc.sagebase.org.s3-website-us-east-1.amazonaws.com/org/sagebionetworks/repo/model/table/SelectColumn.html
case class SelectColumn(
  id: String,
  columnType:	ColumnType.Value,
  name: String
)

// http://hud.rel.rest.doc.sagebase.org.s3-website-us-east-1.amazonaws.com/org/sagebionetworks/repo/model/table/ColumnModel.html
case class ColumnModel(
  id: String,
  columnType: ColumnType.Value,
  name: String,
  maximumSize: Option[Int],
  enumValues: Option[Seq[String]],
  defaultValue: Option[String]
) {
  def toSelectColumn = SelectColumn(id, columnType, name)
}

// http://hud.rel.rest.doc.sagebase.org.s3-website-us-east-1.amazonaws.com/org/sagebionetworks/repo/model/table/DownloadFromTableResult.html
case class DownloadFromTableResult(
  headers: Seq[SelectColumn],
  resultsFileHandleId: String,
  concreteType: String,
  etag: String,
  tableId: String
)

case class FileHandle(
  createdOn: Date,
  id: String,
  concreteType: String,
  contentSize: Int,
  createdBy: String,
  etag: String,
  fileName: String,
  contentType: String,
  contentMd5: String,
  storageLocationId: Option[Int]
)

case class Session(
  sessionToken: String,
  acceptsTermsOfUse: Boolean
)

case class RowReference(
  rowId: Int,
  versionNumber: Int
)

case class RowReferenceSet(
  headers: Seq[SelectColumn],
  etag: Option[String],
  tableId: String,
  rows: Seq[RowReference]
)

case class FileHandleResults(
  list: Seq[FileHandle]
)

case class TableFileHandleResults(
  headers: Seq[SelectColumn],
  tableId: String,
  rows: Seq[FileHandleResults]
)

// http://hud.rel.rest.doc.sagebase.org.s3-website-us-east-1.amazonaws.com/org/sagebionetworks/repo/model/table/PaginatedColumnModels.html
case class PaginatedColumnModels(
  totalNumberOfResults: Int,
  results: Seq[ColumnModel]
)

// http://hud.rel.rest.doc.sagebase.org.s3-website-us-east-1.amazonaws.com/org/sagebionetworks/repo/model/file/FileHandleAssociateType.html
object FileHandleAssociateType extends Enumeration {
  val FileEntity, TableEntity, WikiAttachment, UserProfileAttachment, MessageAttachment, TeamAttachment, SubmissionAttachment, VerificationSubmission = Value
}

// http://hud.rel.rest.doc.sagebase.org.s3-website-us-east-1.amazonaws.com/org/sagebionetworks/repo/model/file/FileHandleAssociation.html
case class FileHandleAssociation(
  associateObjectType: FileHandleAssociateType.Value,
  fileHandleId: String,
  associateObjectId: String
)

// http://hud.rel.rest.doc.sagebase.org.s3-website-us-east-1.amazonaws.com/org/sagebionetworks/repo/model/file/BulkFileDownloadRequest.html
case class BulkFileDownloadRequest(
  requestedFiles: Seq[FileHandleAssociation],
  concreteType: Option[String] = None
)

// http://hud.rel.rest.doc.sagebase.org.s3-website-us-east-1.amazonaws.com/org/sagebionetworks/repo/model/file/FileDownloadSummary.html
case class FileDownloadSummary(
  zipEntryName: Option[String],
  status: String,
  failureMessage: Option[String],
  failureCode: Option[String],
  associateObjectType: FileHandleAssociateType.Value,
  fileHandleId: String,
  associateObjectId: String
)

// http://hud.rel.rest.doc.sagebase.org.s3-website-us-east-1.amazonaws.com/org/sagebionetworks/repo/model/file/BulkFileDownloadResponse.html
case class BulkFileDownloadResponse(
  fileSummary: Seq[FileDownloadSummary],
  concreteType: String,
  userId: String,
  resultZipFileHandleId: String
)

object JsonFormat {
  implicit val AsyncJobStateFormat = EnumFormat(AsyncJobState)
  implicit val AsynchronousJobStatusFormat = Json.format[AsynchronousJobStatus]
  implicit val ColumnTypeFormat = EnumFormat(ColumnType)
  implicit val SelectColumnFormat = Json.format[SelectColumn]
  implicit val ColumnModelFormat = Json.format[ColumnModel]
  implicit val DownloadFromTableResultFormat = Json.format[DownloadFromTableResult]
  implicit val FileHandleFormat = Json.format[FileHandle]
  implicit val SessionFormat = Json.format[Session]
  implicit val RowReferenceFormat = Json.format[RowReference]
  implicit val RowReferenceSetFormat = Json.format[RowReferenceSet]
  implicit val FileHandleResultsFormat = Json.format[FileHandleResults]
  implicit val TableFileHandleResultsFormat = Json.format[TableFileHandleResults]
  implicit val PaginatedColumnModelsFormat = Json.format[PaginatedColumnModels]
  implicit val FileHandleAssociateTypeFormat = EnumFormat(FileHandleAssociateType)
  implicit val FileHandleAssociationFormat = Json.format[FileHandleAssociation]
  implicit val BulkFileDownloadRequestFormat = Json.format[BulkFileDownloadRequest]
  implicit val FileDownloadSummaryFormat = Json.format[FileDownloadSummary]
  implicit val BulkFileDownloadResponseFormat = Json.format[BulkFileDownloadResponse]
}