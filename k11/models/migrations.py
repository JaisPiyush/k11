from k11.models import IndexableArticle, IndexableLinks
import sys

def upgrade():
    IndexableLinks._meta.database.create_tables([IndexableLinks])
    IndexableArticle._meta.database.create_tables([IndexableArticle])

def downgrade():
    IndexableLinks._meta.database.delete_tables([IndexableLinks])
    IndexableArticle._meta.database.delete_tables([IndexableArticle])


if __name__ == "__main__":
    if len(sys.argv) > 1:
        if sys.argv[1] == "upgrade":
            upgrade()
            print('Upgrade Done')
        elif sys.argv[1] == "downgrade":
            downgrade()