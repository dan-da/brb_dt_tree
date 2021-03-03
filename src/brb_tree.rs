// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under the MIT license <LICENSE-MIT
// http://opensource.org/licenses/MIT> or the Modified BSD license <LICENSE-BSD
// https://opensource.org/licenses/BSD-3-Clause>, at your option. This file may not be copied,
// modified, or distributed except according to those terms. Please review the Licences for the
// specific language governing permissions and limitations relating to use of the SAFE Network
// Software.

use crdt_tree::{OpMove, State, TreeId, TreeMeta, TreeReplica};

use brb::BRBDataType;

use serde::Serialize;
use std::{fmt::Debug, hash::Hash};
use thiserror::Error;

/// OpMoveTx
pub type OpMoveTx<ID, M, A> = Vec<OpMove<ID, M, A>>;

/// A BRBDataType wrapper around crdt_tree::State
#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub struct BRBTree<A: Clone + Hash + Ord + Debug, ID: TreeId, M: TreeMeta> {
    actor: A,
    treereplica: TreeReplica<ID, M, A>,
}

impl<
        A: Hash + Ord + Clone + Debug + Serialize + 'static,
        ID: TreeId + Debug + Serialize,
        M: TreeMeta + Eq + Debug + Hash + Serialize,
    > BRBTree<A, ID, M>
{
    /// generates an OpMove.  ie a single tree op.
    pub fn opmove(&self, parent: ID, meta: M, child: ID) -> OpMove<ID, M, A> {
        self.treereplica.opmove(parent, meta, child)
    }

    /// generates an OpMoveTx containing 1 tree op
    pub fn opmovetx(&self, parent: ID, meta: M, child: ID) -> OpMoveTx<ID, M, A> {
        self.treereplica.opmoves(vec![(parent, meta, child)])
    }

    /// generates an OpMoveTx containing N tree ops, each with successive timestamp
    pub fn opmovetx_multi(&self, ops: Vec<(ID, M, ID)>) -> OpMoveTx<ID, M, A> {
        self.treereplica.opmoves(ops)
    }

    /// returns the actor
    pub fn actor(&self) -> &A {
        &self.actor
    }

    /// returns underlying crdt_tree::State object
    pub fn treestate(&self) -> &State<ID, M, A> {
        &self.treereplica.state()
    }

    /// returns underlying crdt_tree::State object
    pub fn treereplica(&self) -> &TreeReplica<ID, M, A> {
        &self.treereplica
    }

    fn validate_opmove(
        &self,
        source: &A,
        op: &OpMove<ID, M, A>,
    ) -> Result<(), <BRBTree<A, ID, M> as BRBDataType<A>>::ValidationError> {
        if op.timestamp().actor_id() != source {
            Err(ValidationError::SourceDoesNotMatchOp)
        } else {
            Ok(())
        }
    }
}

/// An enumeration of possible Validation Errors
#[derive(Error, Debug, PartialEq, Eq)]
pub enum ValidationError {
    #[error("The source actor does not match the actor associated with the operation")]
    SourceDoesNotMatchOp,
}

impl<
        A: Hash + Ord + Clone + Debug + Serialize + 'static,
        ID: TreeId + Debug + Serialize,
        M: TreeMeta + Eq + Debug + Hash + Serialize,
    > BRBDataType<A> for BRBTree<A, ID, M>
{
    type Op = OpMoveTx<ID, M, A>;
    type ValidationError = ValidationError;

    /// Create a new BRBTree
    fn new(actor: A) -> Self {
        BRBTree {
            actor: actor.clone(),
            treereplica: TreeReplica::new(actor),
        }
    }

    /// Validate an operation.
    fn validate(&self, source: &A, op_tx: &Self::Op) -> Result<(), Self::ValidationError> {
        for op in op_tx {
            self.validate_opmove(source, op)?;
        }
        Ok(())
    }

    /// Apply an operation to the underlying Tree datatype
    fn apply(&mut self, op_tx: Self::Op) {
        for op in op_tx {
            self.treereplica.apply_op(op);
        }
    }
}
