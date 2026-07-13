use smallvec::SmallVec;
use nalgebra::{SMatrix, Complex};

// Only 7 is needed as shown by my paper
// Superoperator space
// |H>   [σ σ 0]
// |V>   [σ σ 0]
// |vac> [H V I]
// where σ indicates $\mathcal{M}_2$ pauli errors, H and V are polarization dependent loss, and
// I is the vacuum identity
// This superoperator has 7 degrees of freedom, which can be representable with a 7x7
// $\chi$ matrix, which ultimately reduces down to 7 kraus operators, not 9 as usually
// expected from a qutrit system
// This robust encoding allows for the expression of a lossless operator in the form of an
// extended jones matrix in the form of $J\oplus $I_{1 \times 1}$
pub type PhotonicKrausOperators = SmallVec<[SMatrix<Complex<f32>, 3, 3>; 7]>;
