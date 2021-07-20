"""create account table

Revision ID: f764b0b0e948
Revises: 
Create Date: 2021-07-20 15:12:46.235135

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'f764b0b0e948'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "indexable_links",
        sa.Column("link", sa.String, primary_key=True),
        sa.Column("scraped_on", sa.DateTime, primary_key=False),
        sa.Column('source_name', sa.String, primary_key=True)
    )

    op.create_table(
        "article_indexable_links",
        sa.Column("article_id", sa.String, primary_key=True),
        sa.Column("link", sa.String, primary_key=False, index=True),
        sa.Column("site_name", sa.String, primary_key=False, index=False),
        sa.Column("scraped_on", sa.DateTime, primary_key=False,),
        sa.Column('pub_date', sa.DateTime, primary_key=False)
    )


def downgrade():
    op.drop_table('indexable_links')
    op.drop_table('article_indexable_links')
