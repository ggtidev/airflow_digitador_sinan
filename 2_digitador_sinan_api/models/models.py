from sqlalchemy import Column, Integer, String, ForeignKey, Text
from sqlalchemy.orm import relationship, declarative_base

Base = declarative_base()

class SistemaAlvo(Base):
    __tablename__ = 'sistemas_alvo'

    id = Column(Integer, primary_key=True)
    nome = Column(String(100), nullable=False)
    descricao = Column(String(255), nullable=True)

    agravos = relationship('Agravo', back_populates='sistema_alvo')


class Agravo(Base):
    __tablename__ = 'agravos'

    id = Column(Integer, primary_key=True)
    sistema_alvo_id = Column(Integer, ForeignKey('sistemas_alvo.id'), nullable=False)
    nome = Column(String(100), nullable=False)
    descricao = Column(String(255), nullable=True)

    sistema_alvo = relationship('SistemaAlvo', back_populates='agravos')
    notificacoes = relationship('RpaNotificacao', back_populates='agravo')


class RpaNotificacao(Base):
    __tablename__ = 'rpa_notificacoes'

    id = Column(Integer, primary_key=True)
    num_notificacao = Column(String(7), nullable=False)
    record = Column(String, nullable=False)
    status = Column(String, nullable=False)
    erro_pergunta = Column(String, nullable=True)
    agravo_id = Column(Integer, ForeignKey('agravos.id'), nullable=False)

    agravo = relationship('Agravo', back_populates='notificacoes')
    detalhes = relationship('RpaNotificacaoDetalhe', back_populates='notificacao')


class RpaNotificacaoDetalhe(Base):
    __tablename__ = 'rpa_notificacao_detalhes'

    id = Column(Integer, primary_key=True)
    rpa_notificacao_id = Column(Integer, ForeignKey('rpa_notificacoes.id'), nullable=False)
    field_name = Column(String, nullable=False)
    value = Column(Text, nullable=True)

    notificacao = relationship('RpaNotificacao', back_populates='detalhes')
