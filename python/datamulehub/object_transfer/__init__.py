from .gcs.archive_transfer import transfer_sgml_archive as transfer_sgml_archive_gcs
from .gcs.archive_transfer import transfer_tar_archive as transfer_tar_archive_gcs
from .gcs.archive_transfer import transfer_xbrl_archive as transfer_xbrl_archive_gcs
from .gcs.datasets_transfer import transfer_dataset as transfer_dataset_gcs
from .s3.archive_transfer import transfer_sgml_archive as transfer_sgml_archive_s3
from .s3.archive_transfer import transfer_tar_archive as transfer_tar_archive_s3
from .s3.archive_transfer import transfer_xbrl_archive as transfer_xbrl_archive_s3
from .s3.datasets_transfer import transfer_dataset as transfer_dataset_s3

__all__ = [
    "transfer_dataset_gcs",
    "transfer_dataset_s3",
    "transfer_sgml_archive_gcs",
    "transfer_sgml_archive_s3",
    "transfer_tar_archive_gcs",
    "transfer_tar_archive_s3",
    "transfer_xbrl_archive_gcs",
    "transfer_xbrl_archive_s3",
]
