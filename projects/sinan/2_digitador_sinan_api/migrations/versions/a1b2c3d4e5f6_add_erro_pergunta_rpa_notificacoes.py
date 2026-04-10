"""add coluna erro_pergunta em rpa_notificacoes

Revision ID: a1b2c3d4e5f6
Revises: f04da2dc26a5
Create Date: 2026-03-31 11:56:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a1b2c3d4e5f6'
down_revision: Union[str, Sequence[str], None] = 'f04da2dc26a5'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Adiciona a coluna erro_pergunta na tabela rpa_notificacoes."""
    op.add_column(
        'rpa_notificacoes',
        sa.Column('erro_pergunta', sa.String(), nullable=True)
    )


def downgrade() -> None:
    """Remove a coluna erro_pergunta da tabela rpa_notificacoes."""
    op.drop_column('rpa_notificacoes', 'erro_pergunta')
